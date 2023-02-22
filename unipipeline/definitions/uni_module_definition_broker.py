from typing import Any, Type

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.definitions.uni_module_definition import UniModuleDefinition

uni_broker_template = '''
from typing import Set, List

from unipipeline import UniBroker, UniMessageMeta, UniBrokerConsumer


class {{name}}(UniBroker):

    def stop_consuming(self) -> None:
        raise NotImplementedError('method stop_consuming must be specified for class "{{name}}"')

    def connect(self) -> None:
        raise NotImplementedError('method connect must be specified for class "{{name}}"')

    def close(self) -> None:
        raise NotImplementedError('method close must be specified for class "{{name}}"')

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        raise NotImplementedError('method add_topic_consumer must be specified for class "{{name}}"')

    def start_consuming(self) -> None:
        raise NotImplementedError('method start_consuming must be specified for class "{{name}}"')

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        raise NotImplementedError('method publish must be specified for class "{{name}}"')

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        raise NotImplementedError('method get_topic_approximate_messages_count must be specified for class "{{name}}"')

    def initialize(self, topics: Set[str]) -> None:
        raise NotImplementedError('method initialize must be specified for class "{{name}}"')

'''


class UniModuleDefinitionBroker(UniModuleDefinition[Type[UniBroker[Any]]]):
    ASSIGNED_TYPE: Type[UniBroker[Any]] = UniBroker
    CREATE_TEMPLATE = uni_broker_template
