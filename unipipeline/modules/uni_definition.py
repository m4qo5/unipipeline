from typing import Dict, Type, TypeVar, Any

from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_broker_definition import UniBrokerDefinition
from unipipeline.modules.uni_message_type_definition import UniMessageTypeDefinition
from unipipeline.modules.uni_waiting_definition import UniWaitingDefinition
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

TMessage = TypeVar('TMessage')
T = TypeVar('T')


class UniDefinition:
    def __init__(
        self,
        brokers: Dict[str, UniBrokerDefinition[Any]],
        workers: Dict[str, UniWorkerDefinition],
        waitings: Dict[str, UniWaitingDefinition],
        messages: Dict[str, UniMessageTypeDefinition],
    ) -> None:
        self._brokers = brokers
        self._workers = workers
        self._waitings = waitings
        self._messages = messages
        self._connected_brokers: Dict[str, UniBroker] = dict()
        self._worker_definition_by_type: Dict[Any, UniWorkerDefinition] = dict()

    def get_worker(self, name: str) -> UniWorkerDefinition:
        return self._workers[name]

    @property
    def brokers(self) -> Dict[str, UniBrokerDefinition[Any]]:
        return self._brokers

    @property
    def workers(self) -> Dict[str, UniWorkerDefinition]:
        return self._workers

    @property
    def waitings(self) -> Dict[str, UniWaitingDefinition]:
        return self._waitings

    @property
    def messages(self) -> Dict[str, UniMessageTypeDefinition]:
        return self._messages

    def get_connected_broker_instance(self, name: str) -> UniBroker:
        if name not in self._connected_brokers:
            self._connected_brokers[name] = UniBroker.waiting_for_connection(self._brokers[name])
        return self._connected_brokers[name]

    def wait_related_brokers(self, worker_name: str) -> None:
        broker_names = self._workers[worker_name].get_related_broker_names(self._workers)
        for bn in broker_names:
            self.get_connected_broker_instance(bn)

    def load_workers(self, uni_type: Type[T]) -> None:
        if self._worker_definition_by_type:
            return
        for wd in self._workers.values():
            self._worker_definition_by_type[wd.type.import_class(uni_type)] = wd

    def get_worker_definition_by_type(self, worker_type: Any, uni_type: Type[T]) -> UniWorkerDefinition:
        self.load_workers(uni_type)
        return self._worker_definition_by_type[worker_type]
