import json
import logging
from logging import Logger
from typing import Set, List
from uuid import uuid4

from unipipeline.modules.uni_broker import UniBroker, UniBrokerConsumer
from unipipeline.modules.uni_message_meta import UniMessageMeta


class UniLogBroker(UniBroker):
    def start_consuming(self) -> None:
        self._logger.info(f'{self._logging_prefix} start consuming')

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        return 0

    def initialize(self, topics: Set[str]) -> None:
        self._logger.info(f'{self._logging_prefix} initialized')

    def mk_logger(self) -> Logger:
        return logging.getLogger(__name__)

    def mk_log_prefix(self) -> str:
        return f'{type(self).__name__} {self._uni_definition.name}::{uuid4()} :'

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._logger = self.mk_logger()
        self._logging_prefix = self.mk_log_prefix()

    def connect(self) -> None:
        self._logger.info(f'{self._logging_prefix} connect')

    def close(self) -> None:
        self._logger.info(f'{self._logging_prefix} close')

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        self._logger.info(f'{self._logging_prefix} add consumer "{consumer.id}" to topic "{consumer.topic}" :: {consumer.group_id}')

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        for meta in meta_list:
            self._logger.info(f'{self._logging_prefix} publish {json.dumps(meta.dict())}')
