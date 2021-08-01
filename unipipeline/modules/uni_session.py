import asyncio
from typing import Set, Dict, NamedTuple, Any, List, TYPE_CHECKING

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.config.uni_config import UniConfig
from unipipeline.errors.uni_config_error import UniConfigError
from unipipeline.utils.uni_echo import UniEcho
from unipipeline.utils.uni_util import UniUtil

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator


class UniBrokerInitRecipe(NamedTuple):
    topics: Set[str]
    answer_topics: Set[str]


class UniSession:

    def __init__(self, config: UniConfig, echo: UniEcho, util: UniUtil) -> None:
        self._config = config
        self._echo = echo
        self._util = util

        self._worker_init_list: Set[str] = set()
        self._worker_initialized_list: Set[str] = set()
        self._waiting_init_list: Set[str] = set()
        self._waiting_initialized_list: Set[str] = set()
        self._brokers_with_topics_to_init: Dict[str, UniBrokerInitRecipe] = dict()
        self._brokers_with_topics_initialized: Dict[str, UniBrokerInitRecipe] = dict()

        self._consumer_worker_names: Set[str] = set()

        self._brokers_with_active_consumption: List[UniBroker[Any]] = list()

        self._interrupted = False

    def add_broker_with_active_consumption(self, broker: UniBroker[Any]) -> None:
        self._brokers_with_active_consumption.append(broker)

    def get_consumer_worker_names(self) -> Set[str]:
        return self._consumer_worker_names

    def is_worker_initialized(self, name: str) -> bool:
        return name in self._worker_initialized_list

    def add_worker_to_consume_list(self, name: str) -> None:
        wd = self._config.workers[name]
        if wd.marked_as_external:
            raise OverflowError(f'your could not use worker "{name}" as consumer. it marked as external "{wd.external}"')
        self._consumer_worker_names.add(name)
        self._echo.log_info(f'added consumer {name}')

    def add_worker_to_init_list(self, name: str, no_related: bool) -> None:
        if name not in self._config.workers:
            raise UniConfigError(f'worker "{name}" is not found in config "{self._config.file}"')
        wd = self._config.workers[name]
        if name not in self._worker_initialized_list:
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
                if wn not in self._worker_initialized_list:
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

    async def initialize(self, mediator: 'UniMediator', everything: bool = False, create: bool = True) -> None:
        if everything:
            for wn in mediator.config.workers.keys():
                self.add_worker_to_init_list(wn, no_related=True)
        echo = self._echo.mk_child('initialize')
        for wn in self._worker_init_list:
            echo.log_info(f'worker "{wn}"')
            wd = self._config.workers[wn]
            if not wd.marked_as_external:
                self._config.get_worker_type(wn)
                if wd.answer_message:
                    self._config.get_message_type(wd.answer_message.name)
            self._config.get_message_type(wd.input_message.name)
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

                b = await mediator.wait_for_broker_connection(bn)

                await b.initialize(collection.topics, collection.answer_topics)
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

    def init_cron_producers(self) -> None:
        for task in self._config.cron_tasks.values():
            self.init_producer_worker(task.worker.name)

    def init_producer_worker(self, name: str) -> None:
        self.add_worker_to_init_list(name, no_related=True)

    def init_consumer_worker(self, name: str) -> None:
        self.add_worker_to_init_list(name, no_related=False)
        self.add_worker_to_consume_list(name)

    def init_consumer_worker_group(self, name: str) -> None:
        wns = self._config.get_worker_names_by_group(name)
        for wn in wns:
            self.add_worker_to_init_list(wn, no_related=False)
            self.add_worker_to_consume_list(wn)

    async def interrupt(self) -> None:
        bf = list()
        for b in self._brokers_with_active_consumption:
            bf.append(b.stop_consuming())
            self._echo.log_debug(f'broker "{b.definition.name}" was notified about interruption')
        await asyncio.gather(*bf)
        self._brokers_with_active_consumption = list()
        self._echo.log_info('all brokers was notified about interruption')
