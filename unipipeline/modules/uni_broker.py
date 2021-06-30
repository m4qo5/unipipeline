import logging
from typing import Callable, Any, Set, NamedTuple, List

from unipipeline.modules.uni_broker_definition import UniBrokerDefinition
from unipipeline.modules.uni_message_meta import UniMessageMeta

logger = logging.getLogger(__name__)


class UniBrokerMessageManager:
    def reject(self) -> None:
        raise NotImplementedError(f'method reject must be specified for class "{type(self).__name__}"')

    def ack(self) -> None:
        raise NotImplementedError(f'method acknowledge must be specified for class "{type(self).__name__}"')


class UniBrokerConsumer(NamedTuple):
    id: str
    group_id: str
    message_handler: Callable[[UniMessageMeta, UniBrokerMessageManager], None]


class UniBroker:
    def __init__(self, definition: UniBrokerDefinition[Any]) -> None:
        self._definition = definition

    def connect(self) -> None:
        raise NotImplementedError(f'method connect must be implemented for {type(self).__name__}')

    def close(self) -> None:
        raise NotImplementedError(f'method close must be implemented for {type(self).__name__}')

    def add_topic_consumer(self, topic: str, consumer: UniBrokerConsumer) -> None:
        raise NotImplementedError(f'method consume must be implemented for {type(self).__name__}')

    def start_consuming(self) -> None:
        raise NotImplementedError(f'method start_consuming must be implemented for {type(self).__name__}')

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        raise NotImplementedError(f'method consume must be implemented for {type(self).__name__}')

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        raise NotImplementedError(f'method get_topic_size must be implemented for {type(self).__name__}')

    def initialize(self, topics: Set[str]) -> None:
        raise NotImplementedError(f'method initialize_topic must be implemented for {type(self).__name__}')

    @property
    def definition(self) -> UniBrokerDefinition[Any]:
        return self._definition
