import contextlib
import functools
import threading
import time
import traceback
import urllib.parse
from time import sleep
from typing import Optional, TypeVar, Set, List, NamedTuple, Callable, TYPE_CHECKING, Dict, Tuple, Any, Generator, Union, Type
from urllib.parse import urlparse

import amqp  # type: ignore
from amqp.exceptions import AMQPError, RecoverableChannelError  # type: ignore

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.brokers.uni_broker_consumer import UniBrokerConsumer
from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.definitions.uni_definition import UniDynamicDefinition
from unipipeline.errors import UniAnswerDelayError, UniMessageRejectError
from unipipeline.message.uni_message import UniMessage
from unipipeline.message_meta.uni_message_meta import UniMessageMeta, UniAnswerParams

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator

BASIC_PROPERTIES__HEADER__COMPRESSION_KEY = 'compression'

PUBLISHING_RETRYABLE_ERRORS = (AMQPError, *{RecoverableChannelError, *amqp.Connection.recoverable_connection_errors})

T = TypeVar('T')
TFn = TypeVar('TFn', bound=Callable[..., Any])
TMessage = TypeVar('TMessage', bound=UniMessage)


class UniAmqpPyBrokerConfig(UniDynamicDefinition):
    exchange_name: str = "communication"
    answer_exchange_name: str = "communication_answer"
    heartbeat: int = 60  # in seconds
    prefetch: int = 1
    retry_max_count: int = 100
    retry_delay_s: int = 1
    persistent_message: bool = True

    mandatory_publishing: bool = False


class UniAmqpPyBrokerMsgProps(NamedTuple):
    content_type: Optional[str] = None
    content_encoding: Optional[str] = None
    application_headers: Optional[Dict[str, str]] = None
    delivery_mode: Optional[int] = None
    priority: Optional[int] = None
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    expiration: Optional[str] = None
    message_id: Optional[str] = None
    timestamp: Optional[int] = None
    type: Optional[str] = None
    user_id: Optional[str] = None
    app_id: Optional[str] = None
    cluster_id: Optional[str] = None


class UniAmqpPyBrokerConsumer(NamedTuple):
    id: str
    queue: str
    on_message_callback: Callable[[amqp.Channel, 'amqp.Message'], None]
    consumer_tag: str
    prefetch_count: int


class UniAmqpPyBroker(UniBroker[UniAmqpPyBrokerConfig]):
    config_type = UniAmqpPyBrokerConfig

    def _interacted(self) -> float:
        now = time.time()
        with self._lock_interaction:
            self._last_interaction = now
        return now

    @contextlib.contextmanager
    def _interaction(self) -> Generator[float, Any, Any]:
        now = self._interacted()
        yield now
        self._interacted()

    def _get_topic_approximate_messages_count(self, ch: amqp.Channel, topic: str) -> int:
        result = ch.queue_declare(queue=topic, passive=True)
        self.echo.log_debug(f'topic "{topic}" has messages={result.message_count}, consumers={result.consumer_count}')
        return int(result.message_count)

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        with self._everytime_new_channel(close=True) as get_ch:
            return self._retry_net_interaction(
                'get_topic_approximate_messages_count',
                lambda has_error: self._get_topic_approximate_messages_count(get_ch(has_error), topic),
                retryable_errors=PUBLISHING_RETRYABLE_ERRORS,
            )

    @classmethod
    def get_connection_uri(cls) -> str:
        raise NotImplementedError(f"cls method get_connection_uri must be implemented for class '{cls.__name__}'")

    @functools.cached_property
    def parsed_connection_uri(self) -> urllib.parse.ParseResult:
        return urlparse(url=self.get_connection_uri())

    __slots__ = (
        '_connection',
        '_consumers',
        '_consuming_enabled',
        '_consumer_in_processing',

        '_interrupted',
        '_initialized_exchanges',
        '_initialized_topics',

        '_heartbeat_enabled',
        '_heartbeat_delay',
        '_heartbeat_thread',

        '_last_interaction',
        '_lock_interaction',

        '_free_channels_lock',
        '_free_channels',
    )

    def __init__(self, mediator: 'UniMediator', definition: UniBrokerDefinition) -> None:
        super().__init__(mediator, definition)

        self._consumers: List[UniAmqpPyBrokerConsumer] = list()

        self._connection = None

        self._consuming_enabled = False
        self._consumer_in_processing = False
        self._interrupted = False

        self._initialized_exchanges: Set[str] = set()
        self._initialized_topics: Set[str] = set()

        self._last_interaction = time.time()
        self._lock_interaction: threading.Lock = threading.Lock()
        self._free_channels_lock: threading.Lock = threading.Lock()
        self._free_channels: List[Tuple[float, amqp.Channel]] = list()

        self._heartbeat_enabled = False
        self._heartbeat_delay = max(self.config.heartbeat / 4, 0.2)
        self._heartbeat_thread: Optional[threading.Thread] = None

    def _close_ch(self, ch: amqp.Channel) -> None:
        ch_id = ch.channel_id
        try:
            with self._interaction():
                ch.close()
        except AMQPError as e:  # noqa
            self.echo.log_warning(f'channel {ch_id} :: closing error :: {str(e)}')
        else:
            self.echo.log_debug(f'channel {ch_id} :: closed successfully')

    def _get_or_create_or_del_free_channel(self) -> Tuple[float, amqp.Channel]:
        now = time.time()
        with self._free_channels_lock:
            for _i in range(len(self._free_channels)):
                c = self._free_channels.pop(-1)
                ch_time, ch = c

                if not ch.is_open or ch.is_closing:
                    continue

                time_left = ch_time - now
                ch_id = ch.channel_id
                if time_left <= 0.:  # REMOVE CHANNEL
                    self._close_ch(ch)
                    continue

                self.echo.log_debug(f'channel {ch_id} :: hold (time left {time_left:0.2f}s)')
                return c

        with self._interaction():
            ch = self.connected_connection.channel()

        self.echo.log_debug(f'channel {ch.channel_id} :: new')

        return now + float(self.config.heartbeat), ch

    @contextlib.contextmanager
    def _everytime_new_channel(self, *, close: bool) -> Generator[Callable[[bool], amqp.Channel], Any, Any]:
        c_to_close = []

        def get_ch(force_new: bool) -> amqp.Channel:
            current_c = self._get_or_create_or_del_free_channel()
            c_to_close.append(current_c)
            return current_c[1]

        try:
            yield get_ch
        finally:
            for c in c_to_close:
                self._close_ch(c[1])

    @contextlib.contextmanager
    def _channel(self, *, close: bool) -> Generator[Callable[[bool], amqp.Channel], Any, Any]:
        c_to_close = []
        current_c = None

        def get_ch(force_new: bool) -> amqp.Channel:
            nonlocal current_c
            if force_new or current_c is None:
                current_c = self._get_or_create_or_del_free_channel()
                c_to_close.append(current_c)
            return current_c[1]

        try:
            yield get_ch
        finally:
            if close:
                for c in c_to_close:
                    self._close_ch(c[1])
            else:
                with self._free_channels_lock:
                    for c in c_to_close:
                        self.echo.log_debug(f'channel {c[1].channel_id} :: release')
                        self._free_channels.append(c)

    @property
    def connected_connection(self) -> amqp.Connection:
        self.connect()
        assert self._connection is not None  # only for mypy
        return self._connection

    def _init_exchange(self, ch: amqp.Channel, exchange: str, *, is_answer: bool) -> None:
        if exchange not in self._initialized_exchanges:
            with self._interaction():
                ch.exchange_declare(
                    exchange=exchange,
                    type="direct",
                    passive=False,
                    durable=True,
                    auto_delete=False,
                )
            self._initialized_exchanges.add(exchange)
            self.echo.log_debug(f'channel {ch.channel_id} :: exchange "{exchange}" was initialized')

    def _just_init_topic(self, ch: amqp.Channel, exchange: str, topic: str, *, is_answer: bool) -> Tuple[str, str]:
        self._init_exchange(ch, exchange, is_answer=is_answer)
        with self._interaction():
            ch.queue_declare(queue=topic, durable=not is_answer, auto_delete=is_answer, exclusive=is_answer, passive=False)
            self.echo.log_debug(f'channel {ch.channel_id} :: queue "{topic}" was initialized')
            ch.queue_bind(queue=topic, exchange=exchange, routing_key=topic)
            self.echo.log_debug(f'channel {ch.channel_id} :: queue "{topic}" was bound to exchange "{exchange}"')
        return exchange, topic

    def _init_answer_topic(self, ch: amqp.Channel, topic: str) -> Tuple[str, str]:
        return self._just_init_topic(ch, self.config.answer_exchange_name, topic, is_answer=True)

    def _init_topic(self, ch: amqp.Channel, topic: str, *, cache_create: bool = True) -> Tuple[str, str]:
        if cache_create and topic in self._initialized_topics:
            return self.config.exchange_name, topic
        res = self._just_init_topic(ch, self.config.exchange_name, topic, is_answer=False)
        self._initialized_topics.add(topic)
        return res

    def initialize(self, topics: Set[str], answer_topic: Set[str]) -> None:
        return

    def stop_consuming(self) -> None:
        self._stop_heartbeat_tick()
        if not self._consuming_enabled:
            self.echo.log_debug('consumption exit skipped')
            return
        self._interrupted = True
        if not self._consumer_in_processing:
            self._consuming_enabled = False
            self.echo.log_debug('consumption stopped')
        else:
            self._consumer_in_processing = False
            self._consuming_enabled = False
            self.echo.log_debug('consumption force stopped')

    def _retry_net_interaction(
        self,
        log_prefix: str,
        fn: Callable[[bool], T],
        *,
        delay: Optional[float] = None,
        retryable_errors: Union[Tuple[Type[Exception], ...], Type[Exception]] = AMQPError
    ) -> T:
        delay = float(self.config.retry_delay_s) if delay is None else delay
        reset_delay = delay * 10
        total_count = max(self.config.retry_max_count, 0)
        retry_count_left = total_count
        has_error = False
        while True:
            last_start_time = time.time()
            try:
                with self._interaction():
                    return fn(has_error)
            except retryable_errors as e:
                retry_count_left -= 1
                has_error = True
                self.echo.log_warning(f'{log_prefix} :: error :: {type(e).__name__} :: {str(e)}')
                self.echo.log_warning(f'{log_prefix} :: retry {total_count - retry_count_left}/{total_count}. delay={delay:0.2f}s')

                sleep(delay)

                if time.time() - last_start_time > reset_delay:
                    has_error = False
                    retry_count_left = total_count
                if not retry_count_left:
                    self.echo.log_error(f'{log_prefix} :: max retry count {total_count} was reached')
                    self.echo.log_error(f'{log_prefix} :: exit on error')
                    self.stop_consuming()
                    raise ConnectionError(f'{log_prefix} :: max retry count {total_count} was reached :: {e}')
            except Exception as e:  # noqa
                self.echo.log_error(f'{log_prefix} :: exit on error')
                self.stop_consuming()
                raise

    def _connect(self) -> None:
        if self._connection is None or not self._connection.connected:
            if self._connection is not None:
                try:
                    self._connection.close()
                except AMQPError:
                    pass
                self._connection = None
            with self._interaction():
                self._connection = amqp.Connection(
                    host=f'{self.parsed_connection_uri.hostname}:{self.parsed_connection_uri.port}',
                    password=self.parsed_connection_uri.password,
                    userid=self.parsed_connection_uri.username,
                    heartbeat=self.config.heartbeat,
                )
                assert self._connection is not None  # only for mypy
                self._connection.connect()
                self._connection.send_heartbeat()
            self.echo.log_debug('connected')

    def connect(self) -> None:  # WITH RETRY
        self._retry_net_interaction(
            'connect',
            lambda _1: self._connect(),
            retryable_errors=PUBLISHING_RETRYABLE_ERRORS,
        )

    def close(self) -> None:
        self._stop_heartbeat_tick()

        if self._connection is None or not self._connection.connected or self._connection.is_closing:
            self._connection = None
            self.echo.log_debug('close :: already closed')
            return

        try:
            with self._interaction():
                self._connection.close()
        except (AMQPError, OSError) as e:  # noqa
            self.echo.log_warning(f'close :: error :: {str(e)}')
        self._connection = None
        self.echo.log_debug('close :: done')

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        echo = self.echo.mk_child(f'topic[{consumer.topic}]')
        if self._consuming_enabled:
            raise OverflowError(f'you cannot add consumer dynamically :: tag="{consumer.id}" group_id={consumer.group_id}')

        def consumer_wrapper(ch: amqp.Channel, message: amqp.Message) -> None:
            key = f'consumer "{consumer.id}" :: channel "{ch.channel_id}" :: message "{message.delivery_tag}"'
            self._interacted()
            self._consumer_in_processing = True
            self.echo.log_debug(f'{key} :: received')

            rejected = True
            try:
                get_meta = functools.partial(
                    self.parse_message_body,
                    content=message.body,
                    compression=message.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                    content_type=message.content_type,
                    unwrapped=consumer.unwrapped,
                )
                consumer.message_handler(get_meta)
                rejected = False
            except UniMessageRejectError:
                self.echo.log_debug(f'{key} :: reject :: started')
                with self._interaction():
                    ch.basic_reject(delivery_tag=message.delivery_tag, requeue=True)
                self.echo.log_debug(f'{key} :: reject :: done')
            except Exception as e: # noqa
                traceback.print_exc()
                self.echo.log_error(f'{key} :: {str(e)}')
                raise

            if not rejected:
                self.echo.log_debug(f'{key} :: ack :: started')
                with self._interaction():
                    ch.basic_ack(delivery_tag=message.delivery_tag)
                self.echo.log_debug(f'{key} :: ack :: done')

            self._consumer_in_processing = False
            if self._interrupted:
                self.stop_consuming()
                self._close_ch(ch)
            self.echo.log_debug(f'{key} :: processing finished')

        self._consumers.append(UniAmqpPyBrokerConsumer(
            id=consumer.id,
            queue=consumer.topic,
            on_message_callback=consumer_wrapper,
            consumer_tag=consumer.id,
            prefetch_count=consumer.prefetch_count,
        ))

        echo.log_debug(f'added consumer :: tag="{consumer.id}" group_id={consumer.group_id}')

    def _stop_heartbeat_tick(self) -> None:
        if not self._heartbeat_enabled:
            return
        self._heartbeat_enabled = False
        if self._heartbeat_thread is not None:
            self.echo.log_debug('heartbeat :: disable')
            if self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join()
                self.echo.log_debug('heartbeat :: thread joined')
            self._heartbeat_thread = None
        self.echo.log_debug('heartbeat :: disabled')

    def _start_heartbeat_tick(self) -> None:
        self._stop_heartbeat_tick()
        if self._heartbeat_enabled:
            return
        self._heartbeat_enabled = self._heartbeat_delay > 0

        if not self._heartbeat_enabled:
            return

        if self._heartbeat_thread is None:
            self._heartbeat_thread = threading.Thread(
                name=f'broker-{self.definition.name}-heartbeat',
                target=self._heartbeat_loop_tick,
                daemon=True,
                kwargs=dict(
                    delay_s=self._heartbeat_delay,
                    heartbeat_delay_threshold=self._heartbeat_delay,
                ),
            )

        if not self._heartbeat_thread.is_alive():
            self._heartbeat_thread.start()

    def _heartbeat_loop_tick(self, delay_s: float, heartbeat_delay_threshold: float) -> None:
        self.echo.log_debug(f'heartbeat :: loop started (delay={delay_s:0.2f}s, threshold={heartbeat_delay_threshold:0.2f}s)')
        step_s = 0.1
        sum_steps = 0.

        while self._heartbeat_enabled:  # is False -> stopping this thread loop
            sleep(step_s)
            sum_steps += step_s
            if sum_steps < delay_s:
                continue
            sum_steps = 0
            with self._lock_interaction:
                current_delay = time.time() - self._last_interaction

            if current_delay < heartbeat_delay_threshold:
                self.echo.log_debug(f'heartbeat :: skipped ({current_delay:0.2f}s > {heartbeat_delay_threshold:0.2f}s)')
                continue
            if not self._heartbeat_enabled:
                break
            self.connected_connection.send_heartbeat()
            self.echo.log_debug(f'heartbeat :: tick (since last interaction {current_delay:0.2f}s)')

    def _consuming(self) -> None:
        if not self._consuming_enabled or len(self._consumers) == 0:
            self.echo.log_warning('start_consuming :: has no consumers to start consuming')
            self.stop_consuming()
            return

        self._interrupted = False
        self._consumer_in_processing = False

        self.echo.log_debug('start_consuming')
        self._connect()
        with self._interaction():
            assert self._connection is not None  # only for mypy
            for c in self._consumers:
                ch = self._connection.channel()
                _1, topic = self._init_topic(ch, c.queue, cache_create=False)
                ch.basic_qos(prefetch_count=self.config.prefetch, a_global=False, prefetch_size=0)
                self.echo.log_debug(f'start_consuming :: channel {ch.channel_id} :: set qos (prefetch={c.prefetch_count})')
                ch.basic_consume(queue=topic, callback=functools.partial(c.on_message_callback, ch), consumer_tag=c.consumer_tag)
                self.echo.log_debug(f'start_consuming :: channel {ch.channel_id} :: consumer "{c.consumer_tag}" bound on "{topic}"')

        self._start_heartbeat_tick()
        self.echo.log_debug('start_consuming :: waiting for new messages...')

        while self._consuming_enabled:
            try:
                assert self._connection is not None  # only for mypy
                self._connection.drain_events()
            except Exception as e:  # noqa
                self.echo.log_error(f'consuming loop error :: {e}')
                raise

    def start_consuming(self) -> None:
        self._consuming_enabled = True
        self._retry_net_interaction('consuming', lambda has_error: self._consuming())

    def _has_messages_in_topic(self, ch: amqp.Channel, topic: str, alone: bool) -> bool:
        if alone:
            size = self._get_topic_approximate_messages_count(ch, topic)
            if size > 0:
                self.echo.log_debug(f'sending was skipped, because topic {topic} has messages: {size}>0')
                return True
        return False

    def _publish_ch(self, process_name: str, ch: amqp.Channel, exchange: str, topic: str, meta: UniMessageMeta, props: UniAmqpPyBrokerMsgProps) -> None:
        ch.basic_publish(
            amqp.Message(body=self.serialize_message_body(meta), **props._asdict()),
            exchange=exchange,
            routing_key=topic,
            mandatory=self.config.mandatory_publishing,
            # immediate=self.config.immediate_publishing,
        )
        self.echo.log_debug(f'channel {ch.channel_id} :: {process_name} :: message "{meta.id}" published into "{topic}"')

    def _mk_mesg_props_by_meta(self, meta: UniMessageMeta) -> UniAmqpPyBrokerMsgProps:
        headers = dict()
        if self.definition.compression is not None:
            headers[BASIC_PROPERTIES__HEADER__COMPRESSION_KEY] = self.definition.compression

        ttl_s = meta.real_ttl_s
        expiration = str(ttl_s * 1000) if ttl_s is not None else None

        if meta.need_answer:
            assert meta.answer_params is not None
            return UniAmqpPyBrokerMsgProps(
                content_type=self.definition.content_type,
                content_encoding='utf-8',
                reply_to=self._mk_answer_topic(meta.answer_params),
                correlation_id=str(meta.id),
                delivery_mode=2 if self.config.persistent_message else 0,
                application_headers=headers,
                expiration=expiration,
            )
        return UniAmqpPyBrokerMsgProps(
            content_type=self.definition.content_type,
            content_encoding='utf-8',
            delivery_mode=2 if self.config.persistent_message else 0,
            application_headers=headers,
            expiration=expiration,
        )

    def publish(self, topic: str, meta_list: List[UniMessageMeta], alone: bool = False) -> None:
        with self._everytime_new_channel(close=True) as get_ch:
            exchange, topic = self._retry_net_interaction(
                'publish._init_topic',
                lambda has_error: self._init_topic(get_ch(has_error), topic),
                retryable_errors=PUBLISHING_RETRYABLE_ERRORS,
            )

            if self._retry_net_interaction(
                'publish._alone',
                lambda has_error: self._has_messages_in_topic(get_ch(has_error), topic, alone),
                retryable_errors=PUBLISHING_RETRYABLE_ERRORS,
            ):
                return

            for meta in meta_list:
                props = self._mk_mesg_props_by_meta(meta)
                self._retry_net_interaction(
                    'publish._publish_ch',
                    lambda has_error: self._publish_ch('publish', get_ch(has_error), exchange, topic, meta, props),
                    retryable_errors=PUBLISHING_RETRYABLE_ERRORS,
                )

    def rpc_call(self, topic: str, meta: UniMessageMeta, *, alone: bool = False, max_delay_s: int = 1, unwrapped: bool = False) -> Optional[UniMessageMeta]:
        with self._everytime_new_channel(close=True) as get_ch:
            return self._retry_net_interaction(
                'rpc_call',
                lambda has_error: self._rpc_call(get_ch(has_error), topic, meta, alone=alone, max_delay_s=max_delay_s, unwrapped=unwrapped),
                retryable_errors=PUBLISHING_RETRYABLE_ERRORS,
            )

    def _rpc_call(self, ch: amqp.Channel, topic: str, meta: UniMessageMeta, *, alone: bool = False, max_delay_s: int = 1, unwrapped: bool = False) -> Optional[UniMessageMeta]:
        assert meta.answer_params is not None

        answer_exchange, answer_topic = self._init_answer_topic(ch, self._mk_answer_topic(meta.answer_params))

        exchange, topic = self._init_topic(ch, topic)
        if self._has_messages_in_topic(ch, topic, alone):
            return None
        self._publish_ch('rpc_call :: publish', ch, exchange, topic, meta, self._mk_mesg_props_by_meta(meta))

        started = time.time()
        while True:  # TODO: rewrite it to consumer
            msg: Optional[amqp.Message] = ch.basic_get(queue=answer_topic, no_ack=True)
            current_delay = time.time() - started
            if msg is None:
                if current_delay > max_delay_s:
                    raise UniAnswerDelayError(f'channel {ch.channel_id} :: rpc_call :: waiting :: answer for {answer_exchange}->{answer_topic} reached delay limit. {current_delay:0.2f}s > {max_delay_s}s')
                else:
                    self.echo.log_debug(f'channel {ch.channel_id} :: rpc_call :: waiting :: no answer {current_delay:0.2f}s in {answer_exchange}->{answer_topic}')
            else:
                break
            sleep(0.1)

        self.echo.log_debug(f'channel {ch.channel_id} :: rpc_call :: waiting :: found message in "{answer_topic}"!')

        return self.parse_message_body(
            msg.body,
            compression=(msg.headers or dict()).get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
            content_type=msg.content_type,
            unwrapped=unwrapped,
        )

    def publish_answer(self, answer_params: UniAnswerParams, meta: UniMessageMeta) -> None:
        headers = dict()

        if self.definition.compression is not None:
            headers[BASIC_PROPERTIES__HEADER__COMPRESSION_KEY] = self.definition.compression

        ttl_s = meta.real_ttl_s
        props = UniAmqpPyBrokerMsgProps(
            content_type=self.definition.content_type,
            content_encoding='utf-8',
            delivery_mode=1,
            application_headers=headers,
            expiration=str(ttl_s * 1000) if ttl_s is not None else None,
        )

        with self._everytime_new_channel(close=True) as get_ch:
            self._retry_net_interaction(
                'publish_answer._publish_ch',
                lambda has_error: self._publish_ch('answer :: publish', get_ch(has_error), self.config.answer_exchange_name, self._mk_answer_topic(answer_params), meta, props),
                retryable_errors=PUBLISHING_RETRYABLE_ERRORS,
            )

    def _mk_answer_topic(self, answer_params: UniAnswerParams) -> str:
        return f'{answer_params.topic}.{answer_params.id}'
