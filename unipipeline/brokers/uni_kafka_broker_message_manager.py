from typing import Callable

from unipipeline.brokers.uni_broker_message_manager import UniBrokerMessageManager


class UniKafkaBrokerMessageManager(UniBrokerMessageManager):
    def __init__(self, commit: Callable[[], None]) -> None:
        self._commit = commit

    async def reject(self) -> None:
        pass

    async def ack(self) -> None:
        self._commit()
