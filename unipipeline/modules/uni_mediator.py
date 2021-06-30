import logging
from time import sleep
from typing import Dict, TypeVar, Any, Set

from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_config import UniConfig
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

TMessage = TypeVar('TMessage')
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
        self._waiting_init_list: Set[str] = set()
        self._waiting_initialized_list: Set[str] = set()

        self._consumers_list: Set[str] = set()
        self._brokers_with_topics_to_init: Dict[str, Set[str]] = dict()

        self._brokers_with_topics_initialized: Dict[str, Set[str]] = dict()

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

    def get_worker(self, name: str, singleton: bool = True) -> UniWorker[UniMessage]:
        if not singleton:
            w_def = self._config.workers[name]
            worker_type = w_def.type.import_class(UniWorker)
            logger.info('get_worker :: initialized worker "%s"', name)
            w = worker_type(definition=w_def, mediator=self)
            return w
        if name not in self._worker_instance_indexes:
            self._worker_instance_indexes[name] = self.get_worker(name, singleton=False)
        return self._worker_instance_indexes[name]

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
