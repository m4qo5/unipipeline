import logging
from time import sleep
from typing import Dict, TypeVar, Any, Set, List, Union, Optional, Type

from unipipeline.modules.uni_message_meta import UniMessageMeta
from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_config import UniConfig
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

TWorker = TypeVar('TWorker', bound=UniWorker)

logger = logging.getLogger(__name__)


class UniMediator:
    def __init__(
        self,
        config: UniConfig,
    ) -> None:
        self._config = config
        self._worker_definition_by_type: Dict[Any, UniWorkerDefinition] = dict()
        self._worker_instance_indexes: Dict[str, UniWorker] = dict()
        self._broker_instance_indexes: Dict[str, UniBroker] = dict()
        self._worker_init_list: Set[str] = set()
        self._worker_initiialized_list: Set[str] = set()
        self._waiting_init_list: Set[str] = set()
        self._waiting_initialized_list: Set[str] = set()

        self._consumers_list: Set[str] = set()
        self._brokers_with_topics_to_init: Dict[str, Set[str]] = dict()

        self._brokers_with_topics_initialized: Dict[str, Set[str]] = dict()

        self._message_types: Dict[str, Type[UniMessage]] = dict()

    def get_broker(self, name: str, singleton: bool = True) -> UniBroker:
        if not singleton:
            broker_def = self.config.brokers[name]
            broker_type = broker_def.type.import_class(UniBroker)
            br = broker_type(definition=broker_def)
            return br
        if name not in self._broker_instance_indexes:
            self._broker_instance_indexes[name] = self.get_broker(name, singleton=False)
        return self._broker_instance_indexes[name]

    def add_worker_to_consume_list(self, name: str) -> None:
        self._consumers_list.add(name)
        logger.info('added consumer %s', name)

    def get_message_type(self, name: str) -> Type[UniMessage]:
        if name in self._message_types:
            return self._message_types[name]

        self._message_types[name] = self.config.messages[name].type.import_class(UniMessage)

        return self._message_types[name]

    def send_to(self, worker_name: str, payload: Union[Dict[str, Any], UniMessage], parent_meta: Optional[UniMessageMeta] = None, alone: bool = False) -> None:
        if worker_name not in self._worker_initiialized_list:
            raise OverflowError(f'worker {worker_name} was not initialized')

        wd = self._config.workers[worker_name]

        message_type = self.get_message_type(wd.input_message.name)
        if isinstance(payload, message_type):
            payload_data = payload.dict()
        elif isinstance(payload, dict):
            payload_data = message_type(**payload).dict()
        else:
            raise TypeError(f'data has invalid type.{type(payload).__name__} was given')

        br = self.get_broker(wd.broker.name)

        if alone:
            size = br.get_topic_approximate_messages_count(wd.topic)
            if size != 0:
                logger.info("worker %s skipped, because topic %s has %s messages", wd.name, wd.topic, size)
                return

        if parent_meta is not None:
            meta = parent_meta.create_child(payload_data)
        else:
            meta = UniMessageMeta.create_new(payload_data)

        meta_list = [meta]
        br.publish(wd.topic, meta_list)  # TODO: make it list by default
        logger.info("worker %s sent message to topic '%s':: %s", wd.name, wd.topic, meta_list)

    def start_consuming(self) -> None:
        brokers = set()
        for wn in self._consumers_list:
            w = self.get_worker(wn)
            w.consume()
            logger.info('consumer %s initialized', wn)
            brokers.add(w.definition.broker.name)
        for bn in brokers:
            b = self.get_broker(bn)
            logger.info('broker %s consuming start', bn)
            b.start_consuming()

    def add_worker_to_init_list(self, name: str, no_related: bool) -> None:
        wd = self._config.workers[name]
        self._worker_init_list.add(name)
        for waiting in wd.waitings:
            if waiting.name not in self._waiting_initialized_list:
                self._waiting_init_list.add(waiting.name)
        self.add_broker_topic_to_init(wd.broker.name, wd.topic)
        self.add_broker_topic_to_init(wd.broker.name, wd.error_topic)
        self.add_broker_topic_to_init(wd.broker.name, wd.error_payload_topic)
        if not no_related:
            for wn in wd.output_workers:
                self._worker_init_list.add(wn)
                owd = self._config.workers[wn]
                self.add_broker_topic_to_init(owd.broker.name, owd.topic)

    def add_broker_topic_to_init(self, name: str, topic: str) -> None:
        if name in self._brokers_with_topics_initialized:
            if topic in self._brokers_with_topics_initialized[name]:
                return

        if name not in self._brokers_with_topics_to_init:
            self._brokers_with_topics_to_init[name] = set()

        self._brokers_with_topics_to_init[name].add(topic)

    def initialize(self, create: bool = True) -> None:
        for wn in self._worker_init_list:
            logger.info('initialize :: worker "%s"', wn)
            self._worker_initiialized_list.add(wn)
        self._worker_init_list = set()

        for waiting_name in self._waiting_init_list:
            self._config.waitings[waiting_name].wait()
            logger.info('initialize :: waiting "%s"', waiting_name)
            self._waiting_initialized_list.add(waiting_name)
        self._waiting_init_list = set()

        if create:
            for bn, topics in self._brokers_with_topics_to_init.items():
                b = self.wait_for_broker_connection(bn)

                b.initialize(topics)
                logger.info('initialize :: broker "%s" topics :: %s', b.definition.name, topics)

                if bn not in self._brokers_with_topics_initialized:
                    self._brokers_with_topics_initialized[bn] = set()
                for topic in topics:
                    self._brokers_with_topics_initialized[bn].add(topic)
            self._brokers_with_topics_to_init = dict()

    def get_worker(self, worker: Union[Type['UniWorker[UniMessage]'], str], singleton: bool = True) -> UniWorker[UniMessage]:
        wd = self._config.get_worker_definition(worker)
        if not singleton or wd.name not in self._worker_instance_indexes:
            worker_type = wd.type.import_class(UniWorker)
            logger.info('get_worker :: initialized worker "%s"', wd.name)
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
                logger.info('wait_for_broker_connection :: broker %s connected', br.definition.name)
                return br
            except ConnectionError as e:
                logger.info('wait_for_broker_connection :: broker %s retry to connect [%s/%s] : %s', br.definition.name, try_count, br.definition.retry_max_count, str(e))
                sleep(br.definition.retry_delay_s)
                continue
        raise ConnectionError(f'unavailable connection to {br.definition.name}')
