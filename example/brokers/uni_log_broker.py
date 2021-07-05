
from typing import Set, List

from unipipeline import UniBroker, UniMessageMeta, UniBrokerConsumer


class LogBroker(UniBroker):

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass

    def add_topic_consumer(self, topic: str, consumer: UniBrokerConsumer) -> None:
        pass

    def start_consuming(self) -> None:
        pass

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        pass

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        pass

    def initialize(self, topics: Set[str]) -> None:
        pass
