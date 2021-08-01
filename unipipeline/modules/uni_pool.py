import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, TypeVar, Awaitable

TArg = TypeVar('TArg')
TResult = TypeVar('TResult')


class UniThreadPool:
    def __init__(self, loop: asyncio.AbstractEventLoop, max_threads: int) -> None:
        self._loop = loop
        self._threads_pool = ThreadPoolExecutor(max_workers=max_threads)

    async def run_in_thread(self, fn: Callable[[TArg], TResult], arg: TArg) -> TResult:
        return await self._loop.run_in_executor(self._threads_pool, fn, arg)

    def _run_in_thread_async_wrapper(self, arg: TArg, fn_async: Callable[[TArg], Awaitable[TResult]]) -> TResult:
        local_loop = asyncio.get_event_loop()
        return local_loop.run_until_complete(fn_async(arg))

    async def run_in_thread_async(self, fn_async: Callable[[TArg], Awaitable[TResult]], arg: TArg) -> TResult:
        fn_wr = functools.partial(self._run_in_thread_async_wrapper, fn_async=fn_async)
        return await self._loop.run_in_executor(self._threads_pool, fn_wr, arg)  # type: ignore
