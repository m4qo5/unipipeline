import logging
from typing import Callable, TypeVar, Any

from unipipeline.modules.uni_broker_definition import UniBrokerDefinition
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_meta import UniMessageMeta

TMessage = TypeVar('TMessage', bound=UniMessage)
logger = logging.getLogger(__name__)


class UniBrokerMessageManager:
    def reject(self) -> None:
        raise NotImplementedError(f'method reject must be specified for class "{type(self).__name__}"')

    def ack(self) -> None:
        raise NotImplementedError(f'method acknowledge must be specified for class "{type(self).__name__}"')


class UniBroker:
    def __init__(self, definition: UniBrokerDefinition[Any]) -> None:
        self._definition = definition

    def connect(self) -> None:
        raise NotImplementedError(f'method connect must be implemented for {type(self).__name__}')

    def close(self) -> None:
        raise NotImplementedError(f'method close must be implemented for {type(self).__name__}')

    def consume(
        self,
        topic: str,
        processor: Callable[[UniMessageMeta, UniBrokerMessageManager], None],
        consumer_tag: str,
        worker_name: str,
        prefetch: int = 1,
    ) -> None:
        raise NotImplementedError(f'method consume must be implemented for {type(self).__name__}')

    def publish(self, topic: str, meta: UniMessageMeta) -> None:
        raise NotImplementedError(f'method consume must be implemented for {type(self).__name__}')

    def get_topic_size(self, topic: str) -> int:
        raise NotImplementedError(f'method get_topic_size must be implemented for {type(self).__name__}')

    def initialize_topic(self, topic: str) -> None:
        raise NotImplementedError(f'method initialize_topic must be implemented for {type(self).__name__}')

    @property
    def definition(self) -> UniBrokerDefinition[Any]:
        return self._definition
