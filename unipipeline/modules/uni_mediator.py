from typing import Dict, Type, TypeVar, Any

from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_config import UniConfig
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

TMessage = TypeVar('TMessage')
T = TypeVar('T')


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

    def get_connected_broker_instance(self, name: str) -> UniBroker:
        if name not in self._connected_brokers:
            self._connected_brokers[name] = UniBroker.waiting_for_connection(self.config.brokers[name])
        return self._connected_brokers[name]

    def wait_related_brokers(self, worker_name: str) -> None:
        broker_names = self.config.workers[worker_name].get_related_broker_names(self.config.workers)
        for bn in broker_names:
            self.get_connected_broker_instance(bn)

    def load_workers(self, uni_type: Type[T]) -> None:
        if self._worker_definition_by_type:
            return
        for wd in self.config.workers.values():
            self._worker_definition_by_type[wd.type.import_class(uni_type)] = wd

    def get_worker_definition_by_type(self, worker_type: Any, uni_type: Type[T]) -> UniWorkerDefinition:
        self.load_workers(uni_type)
        return self._worker_definition_by_type[worker_type]
