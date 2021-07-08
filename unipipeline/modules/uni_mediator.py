from time import sleep
from typing import Dict, TypeVar, Any, Set, Union, Optional, Type, List, NamedTuple, Tuple
from uuid import uuid4

from pydantic import ValidationError

from unipipeline.errors import UniPayloadError
from unipipeline.modules.uni_broker import UniBroker, UniBrokerConsumer
from unipipeline.modules.uni_config import UniConfig
from unipipeline.modules.uni_cron_job import UniCronJob
from unipipeline.modules.uni_echo import UniEcho
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_meta import UniMessageMeta
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition
from unipipeline.utils.sig import soft_interruption

TWorker = TypeVar('TWorker', bound=UniWorker)


class UniBrokerInitRecipe(NamedTuple):
    topics: Set[str]
    answer_topics: Set[str]


class UniMediator:
    def __init__(self, config: UniConfig) -> None:
        self._config = config

        self._worker_definition_by_type: Dict[Any, UniWorkerDefinition] = dict()
        self._worker_instance_indexes: Dict[str, UniWorker] = dict()
        self._broker_instance_indexes: Dict[str, UniBroker[Any]] = dict()
        self._worker_init_list: Set[str] = set()
        self._worker_initialized_list: Set[str] = set()
        self._waiting_init_list: Set[str] = set()
        self._waiting_initialized_list: Set[str] = set()

        self._consumers_list: Set[str] = set()
        self._brokers_with_topics_to_init: Dict[str, UniBrokerInitRecipe] = dict()
        self._brokers_with_topics_initialized: Dict[str, UniBrokerInitRecipe] = dict()

        self._message_types: Dict[str, Type[UniMessage]] = dict()

        self._brokers_with_active_consumption: List[UniBroker[Any]] = list()

    @property
    def echo(self) -> UniEcho:
        return self._config.echo

    def set_echo_level(self, level: int) -> None:
        self.echo.level = level

    def get_broker(self, name: str, singleton: bool = True) -> UniBroker:
        if not singleton:
            broker_def = self.config.brokers[name]
            broker_type = broker_def.type.import_class(UniBroker, self.echo)
            br = broker_type(mediator=self, definition=broker_def)
            return br
        if name not in self._broker_instance_indexes:
            self._broker_instance_indexes[name] = self.get_broker(name, singleton=False)
        return self._broker_instance_indexes[name]

    def add_worker_to_consume_list(self, name: str) -> None:
        wd = self._config.workers[name]
        if wd.marked_as_external:
            raise OverflowError(f'your could not use worker "{name}" as consumer. it marked as external "{wd.external}"')
        self._consumers_list.add(name)
        self.echo.log_info(f'added consumer {name}')

    def get_message_type(self, name: str) -> Type[UniMessage]:
        if name in self._message_types:
            return self._message_types[name]

        self._message_types[name] = self.config.messages[name].type.import_class(UniMessage, self.echo)

        return self._message_types[name]

    def answer_to(self, worker_name: str, meta: UniMessageMeta, payload: Optional[Union[Dict[str, Any], UniMessageMeta, UniMessage]]) -> None:
        wd = self._config.workers[worker_name]
        if not wd.need_answer:
            if payload is not None:
                raise UniPayloadError(f'output message must be None because worker {wd.name} has no possibility to send output messages')
            return

        if payload is None:
            raise UniPayloadError('output message must be not empty')

        answ_meta: UniMessageMeta
        if isinstance(payload, UniMessageMeta):
            answ_meta = payload
        else:
            assert wd.output_message is not None
            output_message_type = self.get_message_type(wd.output_message.name)
            payload_msg: UniMessage
            if isinstance(payload, output_message_type):
                payload_msg = payload
            elif isinstance(payload, dict):
                payload_msg = output_message_type(**payload)
            else:
                raise UniPayloadError(f'output message has invalid type. {type(payload).__name__} was given')

            answ_meta = meta.create_child(payload_msg.dict())

        b = self.get_broker(wd.broker.name)

        b.publish_answer(wd.answer_topic, meta.id, answ_meta)
        self.echo.log_info(f'worker {worker_name} answers to {wd.answer_topic}->{meta.id} :: {answ_meta}')

    def send_to(self, worker_name: str, payload: Union[Dict[str, Any], UniMessage], parent_meta: Optional[UniMessageMeta] = None, alone: bool = False) -> Optional[Tuple[UniMessage, UniMessageMeta]]:
        if worker_name not in self._worker_initialized_list:
            raise OverflowError(f'worker {worker_name} was not initialized')

        wd = self._config.workers[worker_name]

        message_type = self.get_message_type(wd.input_message.name)
        try:
            if isinstance(payload, message_type):
                payload_data = payload.dict()
            elif isinstance(payload, dict):
                payload_data = message_type(**payload).dict()
            else:
                raise TypeError(f'data has invalid type.{type(payload).__name__} was given')
        except ValidationError as e:
            raise UniPayloadError(e)

        br = self.get_broker(wd.broker.name)

        if alone:
            size = br.get_topic_approximate_messages_count(wd.topic)
            if size != 0:
                self.echo.log_info(f'sending to worker "{wd.name}" was skipped, because topic {wd.topic} has messages: {size}>0')
                return None

        if parent_meta is not None:
            meta = parent_meta.create_child(payload_data)
        else:
            meta = UniMessageMeta.create_new(payload_data)

        meta_list = [meta]
        br.publish(wd.topic, meta_list)  # TODO: make it list by default
        self.echo.log_info(f"worker {wd.name} sent message to topic '{wd.topic}':: {meta_list}")

        if wd.need_answer:
            assert wd.output_message is not None
            answ_meta = br.get_answer(wd.answer_topic, meta.id, 1)
            answ_message_type = self.get_message_type(wd.output_message.name)
            answ_msg = answ_message_type(**answ_meta.payload)
            return answ_msg, answ_meta
        return None

    def start_cron(self) -> None:
        cron_jobs = UniCronJob.mk_jobs_list(self.config.cron_tasks.values(), self)
        self.echo.log_debug(f'cron jobs defined: {", ".join(cj.task.name for cj in cron_jobs)}')
        while True:
            delay, jobs = UniCronJob.search_next_tasks(cron_jobs)
            if delay is None:
                return
            self.echo.log_debug(f"sleep {delay} seconds before running the tasks: {[cj.task.name for cj in jobs]}")
            if delay > 0:
                sleep(delay)
            self.echo.log_info(f"run the tasks: {[cj.task.name for cj in jobs]}")
            for cj in jobs:
                cj.send()
            sleep(1.1)  # delay for correct next iteration

    def start_consuming(self) -> None:
        brokers = set()
        for wn in self._consumers_list:
            wd = self._config.workers[wn]
            w = self.get_worker(wn)

            br = self.get_broker(wd.broker.name)

            self.echo.log_info(f"worker {wn} start consuming")
            br.add_consumer(UniBrokerConsumer(
                topic=wd.topic,
                id=f'{wn}__{uuid4()}',
                group_id=wn,
                message_handler=w.uni_process_message
            ))

            self.echo.log_info(f'consumer {wn} initialized')
            brokers.add(w.definition.broker.name)

        with soft_interruption(self._handle_interruption, self._handle_force_interruption, self._interruption_err):
            for bn in brokers:
                b = self.get_broker(bn)
                self._brokers_with_active_consumption.append(b)
                self.echo.log_info(f'broker {bn} consuming start')
                b.start_consuming()

    def _interruption_err(self, err: Exception) -> None:
        self.echo.log_error(str(err))
        raise err

    def _handle_force_interruption(self) -> None:
        self.echo.log_warning('force interruption detected')

    def _handle_interruption(self) -> None:
        self.echo.log_warning('interruption detected')
        for b in self._brokers_with_active_consumption:
            self.echo.log_debug(f'broker "{b.definition.name}" was notified about interruption')
            b.stop_consuming()
        self._brokers_with_active_consumption = list()
        self.echo.log_info(f'all brokers was notified about interruption')

    def add_worker_to_init_list(self, name: str, no_related: bool) -> None:
        if name not in self._config.workers:
            self.echo.exit_with_error(f'worker "{name}" is not found in config "{self.config.file}"')
        wd = self._config.workers[name]
        self._worker_init_list.add(name)
        for waiting in wd.waitings:
            if waiting.name not in self._waiting_initialized_list:
                self._waiting_init_list.add(waiting.name)
        self.add_broker_topic_to_init(wd.broker.name, wd.topic, False)
        self.add_broker_topic_to_init(wd.broker.name, wd.error_topic, False)
        self.add_broker_topic_to_init(wd.broker.name, wd.error_payload_topic, False)
        if wd.need_answer:
            self.add_broker_topic_to_init(wd.broker.name, wd.answer_topic, True)
        if not no_related:
            for wn in wd.output_workers:
                self._worker_init_list.add(wn)
                owd = self._config.workers[wn]
                self.add_broker_topic_to_init(owd.broker.name, owd.topic, False)

    def add_broker_topic_to_init(self, name: str, topic: str, is_answer: bool) -> None:
        if name in self._brokers_with_topics_initialized:
            if is_answer:
                if topic in self._brokers_with_topics_initialized[name].answer_topics:
                    return
            else:
                if topic in self._brokers_with_topics_initialized[name].topics:
                    return

        if name not in self._brokers_with_topics_to_init:
            self._brokers_with_topics_to_init[name] = UniBrokerInitRecipe(set(), set())

        if is_answer:
            self._brokers_with_topics_to_init[name].answer_topics.add(topic)
        else:
            self._brokers_with_topics_to_init[name].topics.add(topic)

    def initialize(self, create: bool = True) -> None:
        echo = self.echo.mk_child('initialize')
        for wn in self._worker_init_list:
            echo.log_info(f'worker "{wn}"', )
            self._worker_initialized_list.add(wn)
        self._worker_init_list = set()

        for waiting_name in self._waiting_init_list:
            self._config.waitings[waiting_name].wait(echo)
            echo.log_info(f'waiting "{waiting_name}"')
            self._waiting_initialized_list.add(waiting_name)
        self._waiting_init_list = set()

        if create:
            for bn, collection in self._brokers_with_topics_to_init.items():
                bd = self._config.brokers[bn]

                if bd.marked_as_external:
                    echo.log_debug(f'broker "{bn}" skipped because it external')
                    continue

                b = self.wait_for_broker_connection(bn)

                b.initialize(collection.topics, collection.answer_topics)
                echo.log_info(f'broker "{b.definition.name}" topics :: {collection.topics}')
                if len(collection.answer_topics) > 0:
                    echo.log_info(f'broker "{b.definition.name}" answer topics :: {collection.answer_topics}')

                if bn not in self._brokers_with_topics_initialized:
                    self._brokers_with_topics_initialized[bn] = UniBrokerInitRecipe(set(), set())
                for topic in collection.topics:
                    self._brokers_with_topics_initialized[bn].topics.add(topic)
                for topic in collection.answer_topics:
                    self._brokers_with_topics_initialized[bn].answer_topics.add(topic)
            self._brokers_with_topics_to_init = dict()

    def get_worker(self, worker: Union[Type['UniWorker'], str], singleton: bool = True) -> UniWorker:
        wd = self._config.get_worker_definition(worker)
        if wd.marked_as_external:
            raise OverflowError(f'worker "{worker}" is external. you could not get it')
        if not singleton or wd.name not in self._worker_instance_indexes:
            assert wd.type is not None
            worker_type = wd.type.import_class(UniWorker, self.echo)
            self.echo.log_info(f'get_worker :: initialized worker "{wd.name}"')
            w = worker_type(definition=wd, mediator=self)
        else:
            return self._worker_instance_indexes[wd.name]
        self._worker_instance_indexes[wd.name] = w
        return w

    @property
    def config(self) -> UniConfig:
        return self._config

    def wait_for_broker_connection(self, name: str) -> UniBroker:
        br = self.get_broker(name)
        for try_count in range(br.definition.retry_max_count):
            try:
                br.connect()
                self.echo.log_info(f'wait_for_broker_connection :: broker {br.definition.name} connected')
                return br
            except ConnectionError as e:
                self.echo.log_info(f'wait_for_broker_connection :: broker {br.definition.name} retry to connect [{try_count}/{br.definition.retry_max_count}] : {e}')
                sleep(br.definition.retry_delay_s)
                continue
        raise ConnectionError(f'unavailable connection to {br.definition.name}')
