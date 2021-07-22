from typing import Generic, Type, Any, TypeVar, Optional, Dict, Union, Tuple, TYPE_CHECKING

from unipipeline.errors.uni_sending_to_worker_error import UniSendingToWorkerError
from unipipeline.modules.uni_broker import UniBrokerMessageManager
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_meta import UniMessageMeta, UniMessageMetaErrTopic
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator

TInputMessage = TypeVar('TInputMessage', bound=UniMessage)
TOutputMessage = TypeVar('TOutputMessage', bound=Optional[UniMessage])


class UniWorker(Generic[TInputMessage, TOutputMessage]):
    def __init__(
        self,
        definition: UniWorkerDefinition,
        mediator: 'UniMediator'
    ) -> None:
        self._uni_payload_cache: Optional[TInputMessage] = None
        self._uni_current_meta: Optional[UniMessageMeta] = None
        self._uni_current_manager: Optional[UniBrokerMessageManager] = None
        self._uni_definition = definition
        self._uni_mediator = mediator
        self._uni_echo = self._uni_mediator.echo.mk_child(f'worker[{self._uni_definition.name}]')
        self._uni_input_message_type: Type[TInputMessage] = self._uni_mediator.get_message_type(self._uni_definition.input_message.name)  # type: ignore
        self._uni_output_message_type: Optional[Type[TOutputMessage]] = self._uni_mediator.get_message_type(self._uni_definition.answer_message.name) if self._uni_definition.answer_message is not None else None  # type: ignore
        self._uni_echo_consumer = self._uni_echo.mk_child('consuming')

    @property
    def input_message_type(self) -> Type[TInputMessage]:
        return self._uni_input_message_type

    @property
    def output_message_type(self) -> Optional[Type[TOutputMessage]]:
        return self._uni_output_message_type

    @property
    def worker_definition(self) -> UniWorkerDefinition:
        return self._uni_definition

    @property
    def message_meta(self) -> UniMessageMeta:
        assert self._uni_current_meta is not None
        return self._uni_current_meta

    @property
    def message_manager(self) -> UniBrokerMessageManager:
        assert self._uni_current_manager is not None
        return self._uni_current_manager

    @property
    def message_payload(self) -> TInputMessage:
        return self._uni_payload_cache  # type: ignore

    def handle_message(self, message: TInputMessage) -> Optional[Union[TOutputMessage, Dict[str, Any]]]:
        raise NotImplementedError(f'method handle_message not implemented for {type(self).__name__}')

    def send_to(self, worker: Union[Type['UniWorker[Any, Any]'], str], data: Any, alone: bool = False) -> Optional[Tuple[UniMessage, UniMessageMeta]]:
        if self._uni_current_meta is None:
            raise UniSendingToWorkerError('meta was not defined. incorrect usage of function "send_to"')
        wd = self._uni_mediator.config.get_worker_definition(worker)

        if wd.name not in self._uni_definition.output_workers:
            raise UniSendingToWorkerError(f'worker {wd.name} is not defined in workers->{self._uni_definition.name}->output_workers')

        return self._uni_mediator.send_to(wd.name, data, parent_meta=self._uni_current_meta, alone=alone)

    def uni_process_message(self, meta: UniMessageMeta, manager: UniBrokerMessageManager) -> None:
        self._uni_echo_consumer.log_debug(f"message {meta.id} received :: {meta}")
        self._uni_current_meta = meta
        self._uni_current_manager = manager
        self._uni_payload_cache = None

        err_topic: Optional[UniMessageMetaErrTopic] = UniMessageMetaErrTopic.MESSAGE_PAYLOAD_ERR
        try:
            self._uni_payload_cache = self._uni_input_message_type(**self.message_meta.payload)  # type: ignore
            err_topic = UniMessageMetaErrTopic.HANDLE_MESSAGE_ERR
            result = self.handle_message(self._uni_payload_cache)
            self._uni_mediator.answer_to(self._uni_definition.name, meta, result, self.worker_definition.answer_unwrapped)
            err_topic = None
        except Exception as e:
            assert err_topic is not None
            self._uni_mediator.move_to_error_topic(self._uni_definition, meta, err_topic, e)
            manager.ack()
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

        if err_topic is None and self._uni_definition.ack_after_success:
            manager.ack()

        self._uni_echo_consumer.log_info(f"message {meta.id} processed")
        self._uni_current_meta = None
        self._uni_current_manager = None
        self._uni_payload_cache = None
