import logging
from time import sleep
from typing import Dict, Type, TypeVar, Any

from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_config import UniConfig
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

TMessage = TypeVar('TMessage')
T = TypeVar('T')

logger = logging.getLogger(__name__)


class UniMediator:
    def __init__(
        self,
        config: UniConfig,
    ) -> None:
        self._config = config
        self._connected_brokers: Dict[str, UniBroker] = dict()
        self._worker_definition_by_type: Dict[Any, UniWorkerDefinition] = dict()
        self._worker_instance_indexes: Dict[str, UniWorker] = dict()

    def get_worker(self, name: str, singleton: bool = True) -> UniWorker[UniMessage]:
        if not singleton:
            w_def = self._config.workers[name]
            worker_type = w_def.type.import_class(UniWorker)
            return worker_type(definition=w_def, mediator=self)
        if name not in self._worker_instance_indexes:
            self._worker_instance_indexes[name] = self.get_worker(name, singleton=False)
        return self._worker_instance_indexes[name]

    @property
    def config(self) -> UniConfig:
        return self._config

    def get_connected_broker(self, name: str) -> UniBroker:
        if name in self._connected_brokers:
            return self._connected_brokers[name]

        broker_def = self.config.brokers[name]
        broker_type = broker_def.type.import_class(UniBroker)
        for try_count in range(broker_def.retry_max_count):
            try:
                br = broker_type(definition=broker_def)
                br.connect()
                logger.debug('%s is available', broker_def.name)
                self._connected_brokers[name] = br
                break
            except ConnectionError as e:
                logger.debug('retry connect to broker %s [%s/%s] : %s', broker_def.name, try_count, broker_def.retry_max_count, str(e))
                sleep(broker_def.retry_delay_s)
                continue
        if name not in self._connected_brokers:
            raise ConnectionError(f'unavailable connection to {broker_def.name}')

        return self._connected_brokers[name]

    def wait_related_brokers(self, worker_name: str) -> None:
        broker_names = self.config.workers[worker_name].get_related_broker_names(self.config.workers)
        for bn in broker_names:
            self.get_connected_broker(bn)

    def load_workers(self, uni_type: Type[T]) -> None:
        if self._worker_definition_by_type:
            return
        for wd in self.config.workers.values():
            self._worker_definition_by_type[wd.type.import_class(uni_type)] = wd

    def get_worker_definition_by_type(self, worker_type: Any, uni_type: Type[T]) -> UniWorkerDefinition:
        self.load_workers(uni_type)
        return self._worker_definition_by_type[worker_type]
