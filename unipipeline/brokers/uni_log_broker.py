import json
import logging
from logging import Logger
from typing import Callable
from uuid import uuid4

from unipipeline import UniBroker, UniMessageMeta, UniBrokerMessageManager


class UniLogBroker(UniBroker):
    def mk_logger(self) -> Logger:
        return logging.getLogger(__name__)

    def mk_log_prefix(self) -> str:
        return f'{type(self).__name__} {self._definition.name}::{uuid4()} :'

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._logger = self.mk_logger()
        self._logging_prefix = self.mk_log_prefix()

    def connect(self) -> None:
        self._logger.info(f'{self._logging_prefix} connect')

    def close(self) -> None:
        self._logger.info(f'{self._logging_prefix} close')

    def consume(self, topic: str, processor: Callable[[UniMessageMeta, UniBrokerMessageManager], None], consumer_tag: str, worker_name: str, prefetch: int = 1) -> None:
        self._logger.info(f'{self._logging_prefix} consume {consumer_tag} :: {worker_name}')

    def publish(self, topic: str, meta: UniMessageMeta) -> None:
        self._logger.info(f'{self._logging_prefix} publish {json.dumps(meta.dict())}')
