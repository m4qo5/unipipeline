from collections import deque
from typing import Callable, Dict, TypeVar, Tuple, Optional, Deque, Set, List

from unipipeline.modules.uni_broker import UniBroker, UniBrokerMessageManager, UniBrokerConsumer
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
        self._echo.log_info(str(msg))
        self._msg_counter += 1
        msg_id = self._msg_counter
        self._waiting_for_process.append((msg_id, msg))
        return msg_id

    def move_back_from_reserved(self, msg_id: int) -> None:
        self._echo.log_debug('move_back_from_reserved')
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


class UniMemoryBroker(UniBroker):
    def __init__(self, *args, **kwargs) -> None:
        super(UniMemoryBroker, self).__init__(*args, **kwargs)
        self._consuming_started = False
        self._queues_by_topic: Dict[str, QL] = dict()
        self._consumers_count = 0

        self._extra_test_params = self.definition.configure_dynamic(dict(
            extra="some"
        ))

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        return self._queues_by_topic[topic].messages_to_process_count()

    def initialize(self, topics: Set[str]) -> None:
        for topic in topics:
            self._queues_by_topic[topic] = QL(self.echo.mk_child(f'topic[{topic}]'))

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass

    def add_topic_consumer(self, topic: str, consumer: UniBrokerConsumer) -> None:
        self._consumers_count += 1
        self._queues_by_topic[topic].add_listener(consumer.message_handler, 1)
        self.echo.log_info(f'consumer for topic "{topic}" added with consumer_tag "{consumer.id}"')

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
