import logging
from typing import Generic, Type, Any, TypeVar, Optional
from uuid import uuid4

from unipipeline.modules.uni_broker import UniBrokerMessageManager
from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_meta import UniMessageMeta
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

TMessage = TypeVar('TMessage', bound=UniMessage)
logger = logging.getLogger(__name__)


class UniWorker(Generic[TMessage]):
    def __init__(
        self,
        definition: UniWorkerDefinition,
        index: UniDefinition
    ) -> None:
        self._current_meta: Optional[UniMessageMeta] = None
        self._definition = definition
        self._index = index
        self._message_type: Type[TMessage] = self._definition.input_message.type.import_class(UniMessage)  # type: ignore
        self._consumer_tag: str = f'{self._definition.name}__{uuid4()}'

    def consume(self) -> None:
        self._index.wait_related_brokers(self._definition.name)
        main_broker = self._index.get_connected_broker_instance(self._definition.broker.name)

        self._definition.wait_everything()
        logger.info("worker %s start consuming", self._definition.name)
        main_broker.consume(
            topic=self._definition.topic,
            processor=self.process_message,
            consumer_tag=self._consumer_tag,
            prefetch=self._definition.prefetch,
            worker_name=self._definition.name,
        )

    @property
    def meta(self) -> Optional[UniMessageMeta]:
        return self._current_meta

    def send(self, data: Any, meta: Optional[UniMessageMeta] = None) -> None:
        if isinstance(data, self._message_type):
            pass
        elif isinstance(data, dict):
            data = self._message_type(**data)
        else:
            raise TypeError(f'data has invalid type.{type(data).__name__} was given')
        meta = meta if meta is not None else UniMessageMeta.create_new(data.dict())
        self._index.get_connected_broker_instance(self._definition.broker.name).publish(self._definition.topic, meta)
        logger.info("worker %s sent message %s to %s topic", self._definition.name, meta, self._definition.topic)

    def send_to_worker(self, worker_type: Type['UniWorker[TMessage]'], data: Any) -> None:
        assert issubclass(worker_type, UniWorker)
        w_def = self._index.get_worker_definition_by_type(worker_type, UniWorker)
        w = worker_type(w_def, self._index)
        w.send(data, meta=self._current_meta.create_child(data))

    def process_message(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        logger.debug("worker %s message %s received", self._definition.name, meta)
        payload: TMessage = self._message_type(**meta.payload)

        self._current_meta = meta
        self.handle_message(payload)

        if self._definition.auto_ack:
            manager.ack()

        logger.debug("worker message %s processed", meta)

    def handle_message(self, message: TMessage) -> None:
        raise NotImplementedError(f'method handle_message not implemented for {type(self).__name__}')
