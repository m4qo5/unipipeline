from typing import Callable, Union, Any, Optional, Dict, TypeVar
from uuid import uuid4, UUID

from mypy_extensions import NamedArg

from unipipeline.answer.uni_answer_message import UniAnswerMessage
from unipipeline.message.uni_message import UniMessage
from unipipeline.worker.uni_msg_params import UniSendingParams, UniGettingAnswerParams, default_getting_answer_params, default_sending_params, TUniSendingMessagePayloadUnion

TInputMessage = TypeVar('TInputMessage', bound=UniMessage)
TAnswMessage = TypeVar('TAnswMessage', bound=UniMessage)


class UniWorkerConsumerManager:
    def __init__(
        self,
        send: Callable[[str, TUniSendingMessagePayloadUnion, NamedArg(UniSendingParams, 'params')], None],
        get_answer_from: Callable[[str, TUniSendingMessagePayloadUnion, NamedArg(UniGettingAnswerParams, 'params')], Optional[UniAnswerMessage[UniMessage]]],
    ) -> None:
        self._send = send
        self._get_answer_from = get_answer_from
        self._id = uuid4()

    @property
    def id(self) -> UUID:
        return self._id

    def stop_consuming(self) -> None:
        raise NotImplementedError(f'{type(self).__name__}.stop_consuming was not implemented')  # TODO

    def exit(self) -> None:
        raise NotImplementedError(f'{type(self).__name__}.exit was not implemented')  # TODO

    def get_answer_from(self, worker: str, data: Union[Dict[str, Any], UniMessage], params: UniGettingAnswerParams = default_getting_answer_params) -> Optional[UniAnswerMessage[TAnswMessage]]:
        return self._get_answer_from(worker, data, params=params)  # type: ignore

    def send_to(self, worker: str, data: Union[Dict[str, Any], TInputMessage], params: UniSendingParams = default_sending_params) -> None:
        self._send(worker, data, params=params)
