import logging
from collections import deque
from typing import Callable, Dict, TypeVar, Tuple, Optional, Deque

from unipipeline import UniBroker, UniMessageMeta, UniBrokerMessageManager

logger = logging.getLogger(__name__)


class UniMemoryBrokerMessageManager(UniBrokerMessageManager):
    def __init__(self, ql: 'QL', msg_id: int) -> None:
        self._msg_id = msg_id
        self._ql = ql

    def reject(self) -> None:
        self._ql.move_back_from_reserved(self._msg_id)

    def ack(self) -> None:
        self._ql.mark_as_processed(self._msg_id)


TItem = TypeVar('TItem')


TConsumer = Callable[[UniMessageMeta, UniBrokerMessageManager], None]


class QL:
    def __init__(self, name: str) -> None:
        self._name = name
        self._waiting_for_process: Deque[Tuple[int, UniMessageMeta]] = deque()
        self._in_process: Optional[Tuple[int, UniMessageMeta]] = None

        self._msg_counter: int = 0
        self._lst_counter: int = 0
        self._listeners: Dict[int, Tuple[TConsumer, int]] = dict()

    def add_msg(self, msg: UniMessageMeta) -> int:
        self._msg_counter += 1
        msg_id = self._msg_counter
        self._waiting_for_process.append((msg_id, msg))
        logger.debug('ql(%s).add_msg msg_id=%s', self._name, msg_id)
        return msg_id

    def move_back_from_reserved(self, msg_id: int) -> None:
        logger.debug('ql(%s).move_back_from_reserved', self._name)
        if self._in_process is None:
            return

        (msg_id_, meta) = self._in_process
        if msg_id != msg_id_:
            return

        self._waiting_for_process.appendleft((msg_id, meta))

    def reserve_next(self) -> Tuple[int, UniMessageMeta]:
        if self._in_process is not None:
            return self._in_process

        item = self._waiting_for_process.popleft()
        self._in_process = item
        logger.debug('ql(%s).reserve_next msg_id=%s', self._name, item[0])
        return item

    def mark_as_processed(self, msg_id: int) -> None:
        logger.debug('ql(%s).mark_as_processed msg_id=%s', self._name, msg_id)
        if self._in_process is None:
            return

        (msg_id_, meta) = self._in_process
        if msg_id != msg_id_:
            return

        self._in_process = None

    def add_listener(self, listener: TConsumer, prefetch: int) -> int:
        lsg_id = self._lst_counter
        self._lst_counter += 1
        self._listeners[lsg_id] = (listener, prefetch)
        return lsg_id

    def rm_listener(self, lst_id: int) -> None:
        if lst_id not in self._listeners:
            return
        self._listeners.pop(lst_id)

    def messages_to_process_count(self) -> int:
        return len(self._waiting_for_process) + (0 if self._in_process is None else 1)

    def has_messages_to_process(self) -> bool:
        return self.messages_to_process_count() > 0

    def process_all(self) -> None:
        logger.debug('ql(%s).process_all len_listeners=%s :: messages=%s', self._name, len(self._listeners), self.messages_to_process_count())
        if len(self._listeners) == 0:
            return

        while self.has_messages_to_process():
            for lst_id in self._listeners.keys():
                if not self.has_messages_to_process():
                    break

                if lst_id not in self._listeners:
                    continue

                (lst, prefetch) = self._listeners[lst_id]

                for i in range(prefetch):
                    if not self.has_messages_to_process():
                        break

                    (msg_id, meta) = self.reserve_next()
                    manager = UniMemoryBrokerMessageManager(self, msg_id)

                    logger.info('ql(%s).process_all :: lsg_id=%s :: i=%s :: msg_id=%s :: %s', self._name, lst_id, i, msg_id, meta)
                    lst(meta, manager)
                    logger.debug('ql(%s).process_all len_listeners=%s :: messages=%s', self._name, len(self._listeners), self.messages_to_process_count())


class UniMemoryBroker(UniBroker):
    _queues_by_topic: Dict[str, QL] = dict()

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass

    def _get_ql(self, topic: str) -> QL:
        name = topic
        topic = f'{self.definition.name}:@:@:{topic}'
        if topic not in UniMemoryBroker._queues_by_topic:
            UniMemoryBroker._queues_by_topic[topic] = QL(f'{self.definition.name}->{name}')
        return UniMemoryBroker._queues_by_topic[topic]

    def consume(self, topic: str, processor: TConsumer, consumer_tag: str, worker_name: str, prefetch: int = 1) -> None:
        ql = self._get_ql(topic)
        ql.add_listener(processor, prefetch)
        ql.process_all()

    def publish(self, topic: str, meta: UniMessageMeta) -> None:
        ql = self._get_ql(topic)
        ql.add_msg(meta)
        ql.process_all()
