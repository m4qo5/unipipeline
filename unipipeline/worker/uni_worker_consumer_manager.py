from typing import Callable, Union, Type, Any, Optional, Dict, TYPE_CHECKING, TypeVar, Awaitable
from uuid import uuid4, UUID

from mypy_extensions import VarArg

from unipipeline.answer.uni_answer_message import UniAnswerMessage
from unipipeline.message.uni_message import UniMessage
from unipipeline.modules.uni_thread_pool import UniThreadPool

if TYPE_CHECKING:
    from unipipeline.worker.uni_worker import UniWorker


TInputMessage = TypeVar('TInputMessage', bound=UniMessage)
TAnswMessage = TypeVar('TAnswMessage', bound=UniMessage)
TResult = TypeVar('TResult')


class UniWorkerConsumerManager:
    def __init__(self, pool: UniThreadPool, send: Callable[[Union[Type['UniWorker[Any, Any]'], str], Union[Dict[str, Any], UniMessage], bool, bool], Awaitable[Optional[UniAnswerMessage[UniMessage]]]]) -> None:
        self._send = send
        self._pool = pool
        self._id = uuid4()

    def run_in_thread(self, fn: Callable[[VarArg()], TResult], *args: Any) -> Awaitable[TResult]:
        return self._pool.run_in_thread(fn, *args)

    @property
    def id(self) -> UUID:
        return self._id

    async def stop_consuming(self) -> None:
        pass  # TODO

    async def exit(self) -> None:
        pass  # TODO

    async def get_answer_from(self, worker: Union[Type['UniWorker[TInputMessage, TAnswMessage]'], str], data: Union[Dict[str, Any], TInputMessage]) -> UniAnswerMessage[TAnswMessage]:
        return await self._send(worker, data, False, True)  # type: ignore

    async def send_to(self, worker: Union[Type['UniWorker[Any, Any]'], str], data: Union[Dict[str, Any], UniMessage], alone: bool = False) -> None:
        await self._send(worker, data, alone, False)
