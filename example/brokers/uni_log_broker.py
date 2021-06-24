
from typing import Callable

from unipipeline import UniBroker, UniMessageMeta, UniBrokerMessageManager


class LogBroker(UniBroker):
    def connect(self) -> None:
        raise NotImplementedError('method connect must be specified for class "LogBroker"')

    def close(self) -> None:
        raise NotImplementedError('method close must be specified for class "LogBroker"')

    def consume(self, topic: str, processor: Callable[[UniMessageMeta, UniBrokerMessageManager], None], consumer_tag: str, worker_name: str, prefetch: int = 1) -> None:
        raise NotImplementedError('method consume must be specified for class "LogBroker"')

    def publish(self, topic: str, meta: UniMessageMeta) -> None:
        raise NotImplementedError('method publish must be specified for class "LogBroker"')
