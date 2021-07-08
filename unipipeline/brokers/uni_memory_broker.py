from collections import deque
from datetime import timedelta
from typing import Callable, Dict, TypeVar, Tuple, Optional, Deque, Set, List
from uuid import UUID

from unipipeline.modules.uni_broker import UniBroker, UniBrokerMessageManager, UniBrokerConsumer
from unipipeline.modules.uni_definition import UniDynamicDefinition
from unipipeline.modules.uni_echo import UniEcho
from unipipeline.modules.uni_message_meta import UniMessageMeta


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
    def __init__(self, echo: UniEcho) -> None:
        self._echo = echo
        self._waiting_for_process: Deque[Tuple[int, UniMessageMeta]] = deque()
        self._in_process: Optional[Tuple[int, UniMessageMeta]] = None

        self._msg_counter: int = 0
        self._lst_counter: int = 0
        self._listeners: Dict[int, Tuple[TConsumer, int]] = dict()

    def add(self, msg: UniMessageMeta) -> int:
        self._msg_counter += 1
        msg_id = self._msg_counter
        self._waiting_for_process.append((msg_id, msg))
        self._echo.log_info(f'added msg_id={msg_id} :: {msg}')
        return msg_id

    def get_next(self) -> UniMessageMeta:
        id, answ = self.reserve_next()
        self.mark_as_processed(id)
        return answ

    def move_back_from_reserved(self, msg_id: int) -> None:
        self._echo.log_debug(f'move_back_from_reserved msg_id={msg_id}')
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
        self._echo.log_debug(f'reserve_next msg_id={item[0]}')
        return item

    def mark_as_processed(self, msg_id: int) -> None:
        self._echo.log_debug(f'mark_as_processed msg_id={msg_id}')
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
        self._echo.log_debug(f'process_all len_listeners={len(self._listeners)} :: messages={self.messages_to_process_count()}')
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

                    self._echo.log_info(f'process_all :: lsg_id={lst_id} :: i={i} :: msg_id={msg_id} :: {meta}')
                    lst(meta, manager)
                    self._echo.log_debug(f'process_all len_listeners={len(self._listeners)} :: messages={self.messages_to_process_count()}')


class UniMemoryBroker(UniBroker[UniDynamicDefinition]):

    def stop_consuming(self) -> None:
        pass

    def __init__(self, *args, **kwargs) -> None:
        super(UniMemoryBroker, self).__init__(*args, **kwargs)
        self._consuming_started = False
        self._queues_by_topic: Dict[str, QL] = dict()
        self._consumers_count = 0

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        return self._queues_by_topic[topic].messages_to_process_count()

    def initialize(self, topics: Set[str], answer_topic: Set[str]) -> None:
        for topic in topics:
            self._init_queue(topic)

    def _init_queue(self, topic: str) -> None:
        if topic in self._queues_by_topic:
            return
        self._queues_by_topic[topic] = QL(self.echo.mk_child(f'topic[{topic}]'))

    def _get_answer_topic_name(self, topic: str, answ_id: UUID) -> str:
        return f'answer@{topic}@{answ_id}'

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        self._consumers_count += 1
        self._queues_by_topic[consumer.topic].add_listener(consumer.message_handler, 1)
        self.echo.log_info(f'consumer for topic "{consumer.topic}" added with consumer_tag "{consumer.id}"')

    def start_consuming(self) -> None:
        if self._consuming_started:
            raise OverflowError(f'consuming has already started')

        if self._consumers_count == 0:
            self.echo.log_warning('has no consumers')
            return

        self._consuming_started = True

        self.echo.log_info(f'start consuming')
        for ql in self._queues_by_topic.values():
            ql.process_all()

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        ql = self._queues_by_topic[topic]
        for meta in meta_list:
            ql.add(meta)
        if self._consuming_started:
            ql.process_all()

    def get_answer(self, answer_topic: str, answer_id: UUID, max_delay_s: int) -> UniMessageMeta:
        topic = self._get_answer_topic_name(answer_topic, answer_id)
        self._init_queue(topic)

        answ = self._queues_by_topic[topic].get_next()

        return answ

    def publish_answer(self, answer_topic: str, answer_id: UUID, meta: UniMessageMeta) -> None:
        topic = self._get_answer_topic_name(answer_topic, answer_id)
        self._init_queue(topic)
        self._queues_by_topic[topic].add(meta)
