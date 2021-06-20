import logging
from typing import Generic, Type, Any, TypeVar, Optional, Dict
from uuid import uuid4

from unipipeline.modules.uni_broker import UniBrokerMessageManager
from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_meta import UniMessageMeta, UniMessageMetaErr, UniMessageMetaErrTopic
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

TMessage = TypeVar('TMessage', bound=UniMessage)
logger = logging.getLogger(__name__)


class UniPayloadParsingError(Exception):
    def __init__(self, exception: Exception):
        self.parent_exception = exception


class UniWorker(Generic[TMessage]):
    def __init__(
        self,
        definition: UniWorkerDefinition,
        index: UniDefinition
    ) -> None:
        self._moved = False
        self._payload_cache: Optional[TMessage] = None
        self._current_meta: Optional[UniMessageMeta] = None
        self._current_manager: Optional[UniBrokerMessageManager] = None
        self._definition = definition
        self._index = index
        self._message_type: Type[TMessage] = self._definition.input_message.type.import_class(UniMessage)  # type: ignore
        self._consumer_tag: str = f'{self._definition.name}__{uuid4()}'
        self._worker_instances_for_sending: Dict[Type[UniWorker], UniWorker] = dict()

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
    def meta(self) -> UniMessageMeta:
        assert self._current_meta is not None
        return self._current_meta

    @property
    def manager(self) -> UniBrokerMessageManager:
        assert self._current_manager is not None
        return self._current_manager

    @property
    def payload(self) -> TMessage:
        if self._payload_cache is None:
            try:
                self._payload_cache = self._message_type(**self.meta.payload)
            except Exception as e:
                raise UniPayloadParsingError(e)
        return self._payload_cache

    def send(self, payload: Dict[str, Any], meta: Optional[UniMessageMeta] = None) -> None:
        if isinstance(payload, self._message_type):
            pass
        elif isinstance(payload, dict):
            payload = self._message_type(**payload)
        else:
            raise TypeError(f'data has invalid type.{type(payload).__name__} was given')
        meta = meta if meta is not None else UniMessageMeta.create_new(payload.dict())
        self._index.get_connected_broker_instance(self._definition.broker.name).publish(self._definition.topic, meta)
        logger.info("worker %s sent message %s to %s topic", self._definition.name, meta, self._definition.topic)

    def send_to_worker(self, worker_type: Type['UniWorker[TMessage]'], data: Any) -> None:
        if worker_type not in self._worker_instances_for_sending:
            assert issubclass(worker_type, UniWorker)
            w_def = self._index.get_worker_definition_by_type(worker_type, UniWorker)
            assert w_def.name in self._definition.output_workers
            w = worker_type(w_def, self._index)
            self._worker_instances_for_sending[worker_type] = w

        assert self._current_meta is not None
        meta = self._current_meta.create_child(data)
        self._worker_instances_for_sending[worker_type].send(data, meta=meta)

    def process_message(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        logger.debug("worker %s message %s received", self._definition.name, meta)
        self._moved = False
        self._current_meta = meta
        self._current_manager = manager
        self._payload_cache = None

        unsupported_err_topic = False
        if not meta.has_error:
            try:
                self.handle_message(self.payload)
            except Exception as e:
                self.move_to_error_topic(UniMessageMetaErrTopic.HANDLE_MESSAGE_ERR, e)
        else:
            try:
                if meta.error.error_topic is UniMessageMetaErrTopic.HANDLE_MESSAGE_ERR:
                    self.handle_error_message_handling(self.payload)
                elif meta.error.error_topic is UniMessageMetaErrTopic.MESSAGE_PAYLOAD_ERR:
                    self.handle_error_message_payload(self.meta, self.manager)
                elif meta.error.error_topic is UniMessageMetaErrTopic.ERROR_HANDLING_ERR:
                    self.handle_error_handling(self.meta, self.manager)
                else:
                    unsupported_err_topic = True
            except Exception as e:
                self.move_to_error_topic(UniMessageMetaErrTopic.ERROR_HANDLING_ERR, e)

        if unsupported_err_topic:
            self.move_to_error_topic(UniMessageMetaErrTopic.SYSTEM_ERR, NotImplementedError(f'{meta.error.error_topic} is not implemented in process_message'))

        if not self._moved and self._definition.auto_ack:
            manager.ack()

        logger.debug("worker message %s processed", meta)
        self._moved = False
        self._current_meta = None
        self._current_manager = None
        self._payload_cache = None

    def handle_message(self, message: TMessage) -> None:
        raise NotImplementedError(f'method handle_message not implemented for {type(self).__name__}')

    def move_to_error_topic(self, err_topic: UniMessageMetaErrTopic, err: Exception) -> None:
        self._moved = True
        meta = self.meta.create_error_child(err_topic, err)
        self._index.get_connected_broker_instance(self._definition.broker.name).publish(f'{self._definition.topic}__{err_topic.value}', meta)
        self.manager.ack()

    def handle_error_message_handling(self, message: TMessage) -> None:
        pass

    def handle_error_message_payload(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        pass

    def handle_error_handling(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        pass

    def handle_uni_error(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        pass
