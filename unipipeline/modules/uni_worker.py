import logging
from typing import Generic, Type, Any, TypeVar, Optional, Dict, Union
from uuid import uuid4

from unipipeline.modules.uni_broker import UniBrokerMessageManager
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_meta import UniMessageMeta, UniMessageMetaErrTopic
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
        mediator: Any
    ) -> None:
        from unipipeline.modules.uni_mediator import UniMediator
        self._uni_moved = False
        self._uni_payload_cache: Optional[TMessage] = None
        self._uni_current_meta: Optional[UniMessageMeta] = None
        self._uni_current_manager: Optional[UniBrokerMessageManager] = None
        self._uni_definition = definition
        self._uni_mediator: UniMediator = mediator
        self._uni_message_type: Type[TMessage] = self._uni_definition.input_message.type.import_class(UniMessage)  # type: ignore
        self._uni_consumer_tag: str = f'{self._uni_definition.name}__{uuid4()}'
        self._uni_worker_instances_for_sending: Dict[Type[UniWorker], UniWorker] = dict()

    @property
    def message_type(self) -> Type[TMessage]:
        return self._uni_message_type

    def consume(self) -> None:
        self._uni_mediator.wait_related_brokers(self._uni_definition.name)
        main_broker = self._uni_mediator.get_connected_broker_instance(self._uni_definition.broker.name)

        self._uni_definition.wait_everything()
        logger.info("worker %s start consuming", self._uni_definition.name)
        main_broker.consume(
            topic=self._uni_definition.topic,
            processor=self.process_message,
            consumer_tag=self._uni_consumer_tag,
            prefetch=self._uni_definition.prefetch,
            worker_name=self._uni_definition.name,
        )

    @property
    def meta(self) -> UniMessageMeta:
        assert self._uni_current_meta is not None
        return self._uni_current_meta

    @property
    def manager(self) -> UniBrokerMessageManager:
        assert self._uni_current_manager is not None
        return self._uni_current_manager

    @property
    def payload(self) -> TMessage:
        if self._uni_payload_cache is None:
            try:
                self._uni_payload_cache = self._uni_message_type(**self.meta.payload)
            except Exception as e:
                raise UniPayloadParsingError(e)
        return self._uni_payload_cache

    def send(self, payload: Union[Dict[str, Any], TMessage], meta: Optional[UniMessageMeta] = None) -> None:
        if isinstance(payload, self._uni_message_type):
            payload_data = payload.dict()
        elif isinstance(payload, dict):
            payload_data = self._uni_message_type(**payload).dict()
        else:
            raise TypeError(f'data has invalid type.{type(payload).__name__} was given')
        meta = meta if meta is not None else UniMessageMeta.create_new(payload_data)
        self._uni_mediator.get_connected_broker_instance(self._uni_definition.broker.name).publish(self._uni_definition.topic, meta)
        logger.info("worker %s sent message %s to %s topic", self._uni_definition.name, meta, self._uni_definition.topic)

    def send_to_worker(self, worker_type: Type['UniWorker[TMessage]'], data: Any) -> None:
        if worker_type not in self._uni_worker_instances_for_sending:
            if not issubclass(worker_type, UniWorker):
                raise ValueError(f'worker_type {worker_type.__name__} is not subclass of UniWorker')
            w_def = self._uni_mediator.get_worker_definition_by_type(worker_type, UniWorker)
            if w_def.name not in self._uni_definition.output_workers:
                raise ValueError(f'worker {w_def.name} is not defined in workers->{self._uni_definition.name}->output_workers')
            w = worker_type(w_def, self._uni_mediator)
            self._uni_worker_instances_for_sending[worker_type] = w

        assert self._uni_current_meta is not None
        meta = self._uni_current_meta.create_child(data)
        self._uni_worker_instances_for_sending[worker_type].send(data, meta=meta)

    def process_message(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        logger.debug("worker %s message %s received", self._uni_definition.name, meta)
        self._uni_moved = False
        self._uni_current_meta = meta
        self._uni_current_manager = manager
        self._uni_payload_cache = None

        unsupported_err_topic = False
        if not meta.has_error:
            try:
                self.handle_message(self.payload)
            except Exception as e:
                logger.error(e)
                self.move_to_error_topic(UniMessageMetaErrTopic.HANDLE_MESSAGE_ERR, e)
        else:
            try:
                assert meta.error is not None  # for mypy needs
                if meta.error.error_topic is UniMessageMetaErrTopic.HANDLE_MESSAGE_ERR:
                    self.handle_error_message_handling(self.payload)
                elif meta.error.error_topic is UniMessageMetaErrTopic.MESSAGE_PAYLOAD_ERR:
                    self.handle_error_message_payload(self.meta, self.manager)
                elif meta.error.error_topic is UniMessageMetaErrTopic.ERROR_HANDLING_ERR:
                    self.handle_error_handling(self.meta, self.manager)
                else:
                    unsupported_err_topic = True
            except Exception as e:
                logger.error(e)
                self.move_to_error_topic(UniMessageMetaErrTopic.ERROR_HANDLING_ERR, e)

        if unsupported_err_topic:
            assert meta.error is not None  # for mypy needs
            err = NotImplementedError(f'{meta.error.error_topic} is not implemented in process_message')
            logger.error(err)
            self.move_to_error_topic(UniMessageMetaErrTopic.SYSTEM_ERR, err)

        if not self._uni_moved and self._uni_definition.ack_after_success:
            manager.ack()

        logger.debug("worker message %s processed", meta)
        self._uni_moved = False
        self._uni_current_meta = None
        self._uni_current_manager = None
        self._uni_payload_cache = None

    def handle_message(self, message: TMessage) -> None:
        raise NotImplementedError(f'method handle_message not implemented for {type(self).__name__}')

    def move_to_error_topic(self, err_topic: UniMessageMetaErrTopic, err: Exception) -> None:
        self._uni_moved = True
        meta = self.meta.create_error_child(err_topic, err)
        self._uni_mediator.get_connected_broker_instance(self._uni_definition.broker.name).publish(f'{self._uni_definition.topic}__{err_topic.value}', meta)
        self.manager.ack()

    def handle_error_message_handling(self, message: TMessage) -> None:
        pass

    def handle_error_message_payload(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        pass

    def handle_error_handling(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        pass

    def handle_uni_error(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        pass
