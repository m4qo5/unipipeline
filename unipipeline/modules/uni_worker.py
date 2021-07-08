from typing import Generic, Type, Any, TypeVar, Optional, Dict, Union

from unipipeline.errors import UniSendingToWorkerError
from unipipeline.modules.uni_broker import UniBrokerMessageManager
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_meta import UniMessageMeta, UniMessageMetaErrTopic
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

TMessage = TypeVar('TMessage', bound=UniMessage)


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
        self._uni_consume_initialized = False
        self._uni_payload_cache: Optional[TMessage] = None
        self._uni_current_meta: Optional[UniMessageMeta] = None
        self._uni_current_manager: Optional[UniBrokerMessageManager] = None
        self._uni_definition = definition
        self._uni_mediator: UniMediator = mediator
        self._uni_worker_instances_for_sending: Dict[Type[UniWorker], UniWorker] = dict()
        self._uni_echo = self._uni_mediator.echo.mk_child(f'worker[{self._uni_definition.name}]')
        self._uni_message_type: Type[TMessage] = self._uni_definition.input_message.type.import_class(UniMessage, self._uni_echo)  # type: ignore
        self._uni_echo_consumer = self._uni_echo.mk_child('consuming')
        self._uni_echo_consumer_sending = self._uni_echo_consumer.mk_child('sending')

    @property
    def message_type(self) -> Type[TMessage]:
        return self._uni_message_type

    @property
    def definition(self) -> UniWorkerDefinition:
        return self._uni_definition

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
        return self._uni_payload_cache  # type: ignore

    def handle_message(self, message: TMessage) -> None:
        raise NotImplementedError(f'method handle_message not implemented for {type(self).__name__}')

    def send_to(self, worker: Union[Type['UniWorker[TMessage]'], str], data: Any, alone: bool = False) -> None:
        if self._uni_current_meta is None:
            raise UniSendingToWorkerError(f'meta was not defined. incorrect usage of function "send_to"')
        wd = self._uni_mediator.config.get_worker_definition(worker)

        if wd.name not in self._uni_definition.output_workers:
            raise UniSendingToWorkerError(f'worker {wd.name} is not defined in workers->{self._uni_definition.name}->output_workers')

        self._uni_mediator.send_to(wd.name, data, parent_meta=self._uni_current_meta, alone=alone)

    def uni_process_message(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        self._uni_echo_consumer.log_debug(f"message {meta.id} received :: {meta}")
        self._uni_reset_processing(meta, manager)

        err_topic = UniMessageMetaErrTopic.MESSAGE_PAYLOAD_ERR
        try:
            self._uni_payload_cache = self._uni_message_type(**self.meta.payload)
            err_topic = UniMessageMetaErrTopic.HANDLE_MESSAGE_ERR
            self.handle_message(self._uni_payload_cache)
            err_topic = None
        except Exception as e:
            self._uni_move_to_error_topic(err_topic, e)
        # else:
        #     try:
        #         assert meta.error is not None  # for mypy needs
        #         if meta.error.error_topic is UniMessageMetaErrTopic.HANDLE_MESSAGE_ERR:
        #             self.handle_error_message_handling(self.payload)
        #         elif meta.error.error_topic is UniMessageMetaErrTopic.MESSAGE_PAYLOAD_ERR:
        #             self.handle_error_message_payload(self.meta, self.manager)
        #         elif meta.error.error_topic is UniMessageMetaErrTopic.ERROR_HANDLING_ERR:
        #             self.handle_error_handling(self.meta, self.manager)
        #         else:
        #             unsupported_err_topic = True
        #     except Exception as e:
        #         self._uni_echo_consumer.log_error(str(e))
        #         self.move_to_error_topic(UniMessageMetaErrTopic.ERROR_HANDLING_ERR, e)
        # if unsupported_err_topic:
        #     assert meta.error is not None  # for mypy needs
        #     err = NotImplementedError(f'{meta.error.error_topic} is not implemented in process_message')
        #     self._uni_echo_consumer.log_error(str(err))
        #     self.move_to_error_topic(UniMessageMetaErrTopic.SYSTEM_ERR, err)

        if not self._uni_moved and self._uni_definition.ack_after_success:
            manager.ack()

        self._uni_echo_consumer.log_info(f"message {meta.id} processed")
        self._uni_reset_processing()

    def _uni_reset_processing(self, meta: UniMessageMeta = None, manager: UniBrokerMessageManager = None) -> None:
        self._uni_moved = False
        self._uni_current_meta = meta
        self._uni_current_manager = manager
        self._uni_payload_cache = None

    def _uni_move_to_error_topic(self, err_topic: UniMessageMetaErrTopic, err: Exception) -> None:
        self._uni_echo_consumer.log_error(str(err))
        self._uni_moved = True
        meta = self.meta.create_error_child(err_topic, err)
        br = self._uni_mediator.get_broker(self._uni_definition.broker.name)
        error_topic = self.definition.error_topic
        if error_topic is UniMessageMetaErrTopic.MESSAGE_PAYLOAD_ERR:
            error_topic = self.definition.error_payload_topic
        br.publish(error_topic, [meta])
        self.manager.ack()
