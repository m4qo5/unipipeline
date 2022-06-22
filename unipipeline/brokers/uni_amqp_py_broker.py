import functools
import threading
import time
import urllib.parse
from time import sleep
from typing import Optional, TypeVar, Set, List, NamedTuple, Callable, TYPE_CHECKING, Dict, Tuple, Any, cast, Type
from urllib.parse import urlparse

import amqp  # type: ignore
from amqp import Channel
from amqp.exceptions import ConnectionError as AqmpConnectionError, RecoverableChannelError, AMQPError  # type: ignore

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.brokers.uni_broker_consumer import UniBrokerConsumer
from unipipeline.brokers.uni_broker_message_manager import UniBrokerMessageManager
from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.definitions.uni_definition import UniDynamicDefinition
from unipipeline.errors import UniAnswerDelayError
from unipipeline.message.uni_message import UniMessage
from unipipeline.message_meta.uni_message_meta import UniMessageMeta, UniAnswerParams
from unipipeline.utils.uni_echo import UniEcho

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator

BASIC_PROPERTIES__HEADER__COMPRESSION_KEY = 'compression'

RECOVERABLE_ERRORS = tuple({AqmpConnectionError, RecoverableChannelError, *amqp.Connection.recoverable_connection_errors})


TMessage = TypeVar('TMessage', bound=UniMessage)

# logging.getLogger('amqp').setLevel(logging.DEBUG)

T = TypeVar('T')


HEADER_TTL = 'x-message-ttl'


class UniPyPikaBrokerMessageManager(UniBrokerMessageManager):

    def __init__(self, channel: amqp.Channel, delivery_tag: str, reject: Callable[[amqp.Channel, str], None], ack: Callable[[amqp.Channel, str], None]) -> None:
        self._channel = channel
        self._delivery_tag = delivery_tag
        self._reacted = False
        self._reject = reject
        self._ack = ack

    def reject(self) -> None:
        if self._reacted:
            return
        self._reacted = True
        self._reject(self._channel, self._delivery_tag)

    def ack(self) -> None:
        if self._reacted:
            return
        self._reacted = True
        self._ack(self._channel, self._delivery_tag)


class UniAmqpPyBrokerConfig(UniDynamicDefinition):
    exchange_name: str = "communication"
    answer_exchange_name: str = "communication_answer"
    heartbeat: int = 60
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


TFn = TypeVar('TFn', bound=Callable[..., Any])


def retryable(
    fn: TFn,
    echo: UniEcho,
    *,
    retryable_errors: Tuple[Type[Exception], ...],
    on_retries_ends: Optional[Callable[[], None]] = None,
    retry_max_count: int = 3,
    retry_delay_s: int = 1,
) -> TFn:
    echo = echo.mk_child(fn.__name__)
    max_retries = max(retry_max_count, 1)
    retry_threshold_s = retry_delay_s * max_retries
    assert retry_delay_s >= 0

    @functools.wraps(fn)
    def fn_wrapper(*args: Any, **kwargs: Any) -> Any:
        retry_counter = 0
        while True:
            starts_at = time.time()
            try:
                res = fn(*args, **kwargs)
                retry_counter = 0
                return res
            except retryable_errors as e:
                echo.log_warning(f'found error :: {e}')
                if int(time.time() - starts_at) >= retry_threshold_s:
                    retry_counter = 0
                if retry_counter >= max_retries:
                    echo.log_error(f'max retries {max_retries} was reached :: {e}')
                    if on_retries_ends is not None:
                        return on_retries_ends()
                    raise
                retry_counter += 1
                if retry_delay_s > 0:
                    sleep(retry_delay_s)
                echo.log_warning(f'retry {retry_counter}/{max_retries} :: {e}')
    return cast(TFn, fn_wrapper)


class UniAmqpPyBroker(UniBroker[UniAmqpPyBrokerConfig]):
    config_type = UniAmqpPyBrokerConfig

    def _interact(self) -> None:
        with self._lock_interaction:
            self._last_interaction = time.time()

    def _get_topic_approximate_messages_count(self, ch: amqp.Channel, topic: str) -> int:
        self._interact()
        res = ch.queue_declare(queue=topic, passive=True)
        self._interact()
        self.echo.log_debug(f'topic "{topic}" has messages={res.message_count}, consumers={res.consumer_count}')
        return int(res.message_count)

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        with self._new_channel() as ch:
            return self._get_topic_approximate_messages_count(ch, topic)

    @classmethod
    def get_connection_uri(cls) -> str:
        raise NotImplementedError(f"cls method get_connection_uri must be implemented for class '{cls.__name__}'")

    @functools.cached_property
    def parsed_connection_uri(self) -> urllib.parse.ParseResult:
        return urlparse(url=self.get_connection_uri())

    def _connection_instance(self) -> amqp.Connection:
        return amqp.Connection(
            host=f'{self.parsed_connection_uri.hostname}:{self.parsed_connection_uri.port}',
            password=self.parsed_connection_uri.password,
            userid=self.parsed_connection_uri.username,
            heartbeat=self.config.heartbeat,
        )

    __slots__ = (
        '_consumers',
        '_connection',
        '_consuming_enabled',
        '_in_processing',
        '_interrupted',
        '_initialized_exchanges',
        '_initialized_topics',
        '_retry_consuming',
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
        self._current_ch: Optional[int] = None

        self._connection = self._connection_instance()

        self._consuming_enabled = False
        self._in_processing = False
        self._interrupted = False

        self._initialized_exchanges: Set[str] = set()
        self._initialized_topics: Set[str] = set()

        self._last_interaction = time.time()
        self._lock_interaction: threading.Lock = threading.Lock()
        self._free_channels_lock: threading.Lock = threading.Lock()
        self._free_channels: List[amqp.Channel] = list()

        def raise_connection_err() -> None:
            raise ConnectionError()

        self._publish_ch_publishing_r = retryable(
            self._publish_ch,
            self.echo.mk_child('publishing'),
            retry_max_count=self.config.retry_max_count,
            retry_delay_s=self.config.retry_delay_s,
            retryable_errors=RECOVERABLE_ERRORS,
            on_retries_ends=raise_connection_err,
        )
        self._new_channel = retryable(  # type: ignore
            self._new_channel,
            self.echo.mk_child('new_channel'),
            retry_max_count=self.config.retry_max_count,
            retry_delay_s=self.config.retry_delay_s,
            retryable_errors=RECOVERABLE_ERRORS,
            on_retries_ends=raise_connection_err,
        )
        self.get_topic_approximate_messages_count = retryable(  # type: ignore
            self.get_topic_approximate_messages_count,
            self.echo.mk_child('get_topic_approximate_messages_count'),
            retry_max_count=self.config.retry_max_count,
            retry_delay_s=self.config.retry_delay_s,
            retryable_errors=RECOVERABLE_ERRORS,
            on_retries_ends=raise_connection_err,
        )
        self.rpc_call = retryable(  # type: ignore
            self.rpc_call,
            self.echo.mk_child('rpc_call'),
            retry_max_count=self.config.retry_max_count,
            retry_delay_s=self.config.retry_delay_s,
            retryable_errors=RECOVERABLE_ERRORS,
            on_retries_ends=raise_connection_err,
        )
        self._retry_consuming = retryable(
            self._consuming,
            self.echo.mk_child('consuming'),
            retry_max_count=self.config.retry_max_count,
            retry_delay_s=self.config.retry_delay_s,
            retryable_errors=RECOVERABLE_ERRORS,
            on_retries_ends=raise_connection_err,
        )
        self._ack = retryable(  # type: ignore
            self._ack,
            self.echo.mk_child('ack'),
            retry_max_count=self.config.retry_max_count,
            retry_delay_s=self.config.retry_delay_s,
            retryable_errors=RECOVERABLE_ERRORS,
            on_retries_ends=raise_connection_err,
        )
        self._reject = retryable(  # type: ignore
            self._reject,
            self.echo.mk_child('reject'),
            retry_max_count=self.config.retry_max_count,
            retry_delay_s=self.config.retry_delay_s,
            retryable_errors=RECOVERABLE_ERRORS,
            on_retries_ends=raise_connection_err,
        )
        self.publish_answer = retryable(  # type: ignore
            self.publish_answer,
            self.echo.mk_child('publish_answer'),
            retry_max_count=self.config.retry_max_count,
            retry_delay_s=self.config.retry_delay_s,
            retryable_errors=RECOVERABLE_ERRORS,
            on_retries_ends=raise_connection_err,
        )

        self._heartbeat_enabled = False
        self._heartbeat_delay = max(self.config.heartbeat / 4, 0.2)
        self._heartbeat_thread: threading.Thread = threading.Thread(
            name=f'broker-{self.definition.name}-system',
            target=self._heartbeat_tick_loop,
            daemon=False,
            kwargs=dict(
                delay_s=self._heartbeat_delay,
                heartbeat_delay_threshold=self._heartbeat_delay,
            ),
        )

    def _hold_channel(self) -> Optional[amqp.Channel]:
        with self._free_channels_lock:
            if len(self._free_channels) > 0:
                ch = self._free_channels.pop()
                if ch.is_open and not ch.is_closing:
                    return ch
        return self._new_channel()

    def _release_channel(self, ch: amqp.Channel) -> None:
        with self._free_channels_lock:
            self._free_channels.append(ch)

    def _cleanup_free_channels(self) -> None:
        with self._free_channels_lock:
            for ch in self._free_channels:
                try:
                    if ch.is_open and not ch.is_closing:
                        ch.close()
                except Exception as e: # noqa
                    pass  # do nothing
                self._interact()
            self._free_channels = list()

    @property
    def connected_connection(self) -> amqp.Connection:
        self.connect()
        return self._connection

    def _init_topic(self, ch: amqp.Channel, exchange: str, topic: str) -> str:
        q_key = f'{exchange}->{topic}'
        if q_key in self._initialized_topics:
            return topic
        self._initialized_topics.add(q_key)

        if exchange not in self._initialized_topics:
            self._initialized_exchanges.add(exchange)
            ch.exchange_declare(
                exchange=exchange,
                type="direct",
                passive=False,
                durable=True,
                auto_delete=False,
            )
            self.echo.log_info(f'exchange "{exchange}" initialized')

        if exchange == self.config.exchange_name:
            ch.queue_declare(queue=topic, durable=True, auto_delete=False, passive=False)
        elif exchange == self.config.answer_exchange_name:
            ch.queue_declare(queue=topic, durable=False, auto_delete=True, exclusive=True, passive=False)
        else:
            raise TypeError(f'invalid exchange name "{exchange}"')

        self._interact()
        ch.queue_bind(queue=topic, exchange=exchange, routing_key=topic)
        self._interact()
        self.echo.log_info(f'queue "{q_key}" initialized')
        return topic

    def initialize(self, topics: Set[str], answer_topic: Set[str]) -> None:
        return

    def stop_consuming(self) -> None:
        if not self._consuming_enabled:
            return
        self._interrupted = True
        if not self._in_processing:
            # self.close()
            self._consuming_enabled = False
            self.echo.log_info('consumption stopped')

        self._stop_heartbeat_tick()

    def connect(self) -> None:
        try:
            if not self._connection.connected:
                self._interact()
                self._connection = self._connection_instance()
                self._connection.connect()
                self._connection.send_heartbeat()
                self.echo.log_info('connected')
                self._interact()
        except RECOVERABLE_ERRORS as e:
            self.echo.log_error(f'connection problem :: {e}')
            raise ConnectionError(str(e))

    def close(self) -> None:
        self.echo.log_info('closing')
        self._stop_heartbeat_tick()

        if not self._connection.connected or self._connection.is_closing:
            self.echo.log_info('already closed')
            return

        try:
            self._interact()
            self._connection.close()
            self._interact()
        except AMQPError:
            pass
        self.echo.log_info('closed')

    def _ack(self, ch: amqp.Channel, delivery_tag: str) -> None:
        self._interact()
        ch.basic_ack(delivery_tag=delivery_tag)
        self._interact()
        self.echo.log_info(f'message "{delivery_tag}" ACK')

    def _reject(self, ch: amqp.Channel, delivery_tag: str) -> None:
        self._interact()
        ch.basic_reject(delivery_tag=delivery_tag, requeue=True)
        self._interact()
        self.echo.log_warning(f'message "{delivery_tag}" REJECT')

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        echo = self.echo.mk_child(f'topic[{consumer.topic}]')
        if self._consuming_enabled:
            raise OverflowError(f'you cannot add consumer dynamically :: tag="{consumer.id}" group_id={consumer.group_id}')

        def consumer_wrapper(ch: amqp.Channel, message: amqp.Message) -> None:
            self._interact()
            self._in_processing = True
            self._current_ch = ch.channel_id
            self.echo.log_info(f'message "{message.delivery_tag}" received from topic "{consumer.topic}" (in consumer {consumer.id})')

            manager = UniPyPikaBrokerMessageManager(ch, message.delivery_tag, reject=self._reject, ack=self._ack)

            try:
                get_meta = functools.partial(
                    self.parse_message_body,
                    content=message.body,
                    compression=message.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                    content_type=message.content_type,
                    unwrapped=consumer.unwrapped,
                )
                consumer.message_handler(get_meta, manager)
            except Exception as e: # noqa
                self.echo.log_error(f'message "{message.delivery_tag}" :: {e}')
                manager.reject()
                self._current_ch = None
                raise

            self._in_processing = False
            if self._interrupted:
                self.stop_consuming()
            self.echo.log_info(f'message "{message.delivery_tag}" :: processing finished')
            self._current_ch = None

        self._consumers.append(UniAmqpPyBrokerConsumer(
            id=consumer.id,
            queue=consumer.topic,
            on_message_callback=consumer_wrapper,
            consumer_tag=consumer.id,
            prefetch_count=consumer.prefetch_count,
        ))

        echo.log_info(f'added consumer :: tag="{consumer.id}" group_id={consumer.group_id}')

    def _stop_heartbeat_tick(self) -> None:
        if not self._heartbeat_enabled:
            return
        self._heartbeat_enabled = False
        if self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join()

    def _start_heartbeat_tick(self) -> None:
        if self._heartbeat_enabled:
            return
        self._heartbeat_enabled = self._heartbeat_delay > 0

        if not self._heartbeat_enabled:
            self.echo.log_info('heartbeat disabled')
            return

        self.echo.log_info(f'heartbeat enabled :: delay {self._heartbeat_delay}s')
        self._heartbeat_thread.start()

    def _heartbeat_tick_loop(self, delay_s: float, heartbeat_delay_threshold: float) -> None:
        while self._heartbeat_enabled:
            sleep(delay_s)
            now = time.time()

            with self._lock_interaction:
                current_delay = now - self._last_interaction

            try:
                self._cleanup_free_channels()

            except Exception as e:  # noqa
                self.echo.log_warning(f'ignored cleanup channels error :: {e}')
                # do nothing

            try:
                if current_delay < heartbeat_delay_threshold:
                    self.echo.log_debug(f'heartbeat tick :: skipped :: {current_delay:0.2f}s > {heartbeat_delay_threshold:0.2f}s')
                    continue
                self._interact()
                self._connection.send_heartbeat()
                self.echo.log_debug(f'heartbeat tick :: waiting for interaction {current_delay:0.2f}s')
                self._interact()

            except RECOVERABLE_ERRORS as e:
                self.echo.log_warning(f'ignored heartbeat error :: {e}')
                continue

            except Exception as e:  # noqa
                self.echo.log_error(str(e))
                self._heartbeat_enabled = False
                return

    def _consuming(self) -> None:
        echo = self.echo.mk_child('consuming')
        if len(self._consumers) == 0:
            echo.log_warning('has no consumers to start consuming')
            return

        self.connect()

        for c in self._consumers:
            ch = self._new_channel()
            topic = self._init_topic(ch, self.config.exchange_name, c.queue)
            echo.log_debug(f'added consumer {c.consumer_tag} on {self.config.exchange_name}->{topic}. prefetch: {self.config.prefetch}')
            ch.basic_qos(prefetch_count=self.config.prefetch, a_global=False, prefetch_size=0)
            self._start_heartbeat_tick()
            echo.log_info(f'starting consuming :: consumers_count={len(self._consumers)}')
            ch.basic_consume(queue=topic, callback=functools.partial(c.on_message_callback, ch), consumer_tag=c.consumer_tag)

        while self._consuming_enabled:
            echo.log_debug('wait for next message ...')
            self._connection.drain_events()

        self._stop_heartbeat_tick()

    def start_consuming(self) -> None:
        if self._consuming_enabled:
            return
        self._consuming_enabled = True
        self._interrupted = False
        self._in_processing = False

        self._retry_consuming()

    def _alone(self, ch: amqp.Channel, topic: str, alone: bool) -> bool:
        if alone:
            size = self._get_topic_approximate_messages_count(ch, topic)
            if size > 0:
                self.echo.log_info(f'sending was skipped, because topic {topic} has messages: {size}>0')
                return True
        return False

    def _publish_ch(self, ch: amqp.Channel, exchange: str, topic: str, meta: UniMessageMeta, props: UniAmqpPyBrokerMsgProps, alone: bool = False) -> None:
        self.echo.log_debug(f'message start publishing to {exchange}->{topic}')

        if not ch.is_open:
            ch = self._new_channel()

        topic = self._init_topic(ch, exchange, topic)

        if self._alone(ch, topic, alone):
            return

        self._interact()
        ch.basic_publish(
            amqp.Message(body=self.serialize_message_body(meta), **props._asdict()),
            exchange=exchange,
            routing_key=topic,
            mandatory=self.config.mandatory_publishing,
            # immediate=self.config.immediate_publishing,
        )
        self._interact()

        self.echo.log_debug(f'message published to {exchange}->{topic}')

    def _mk_mesg_props_by_meta(self, meta: UniMessageMeta) -> UniAmqpPyBrokerMsgProps:
        headers = dict()
        if self.definition.compression is not None:
            headers[BASIC_PROPERTIES__HEADER__COMPRESSION_KEY] = self.definition.compression

        ttl_s = meta.real_ttl_s
        if ttl_s is not None:
            headers[HEADER_TTL] = str(ttl_s * 1000)

        if meta.need_answer:
            assert meta.answer_params is not None
            return UniAmqpPyBrokerMsgProps(
                content_type=self.definition.content_type,
                content_encoding='utf-8',
                reply_to=f'{meta.answer_params.topic}.{meta.answer_params.id}',
                correlation_id=str(meta.id),
                delivery_mode=2 if self.config.persistent_message else 0,
                application_headers=headers,
            )
        return UniAmqpPyBrokerMsgProps(
            content_type=self.definition.content_type,
            content_encoding='utf-8',
            delivery_mode=2 if self.config.persistent_message else 0,
            application_headers=headers,
        )

    def _new_channel(self) -> Channel:  # retryable
        self._interact()
        ch = self.connected_connection.channel()
        self._interact()
        return ch

    def publish(self, topic: str, meta_list: List[UniMessageMeta], alone: bool = False) -> None:
        ch = self._new_channel()
        for meta in meta_list:
            props = self._mk_mesg_props_by_meta(meta)
            self._publish_ch_publishing_r(ch, self.config.exchange_name, topic, meta, props, alone)
        if ch.is_open:
            try:
                ch.close()
            except Exception:  # noqa: B902
                pass  # TODO: handle it
        self.echo.log_info(f'{len(meta_list)} messages published to {self.config.exchange_name}->{topic}')

    def rpc_call(self, topic: str, meta: UniMessageMeta, *, alone: bool = False, max_delay_s: int = 1, unwrapped: bool = False) -> Optional[UniMessageMeta]:
        self.echo.log_debug('rpc_call :: start')
        assert meta.answer_params is not None

        active_ch = self._current_ch is not None

        ch = self.connected_connection.channel(channel_id=self._current_ch) if active_ch else self._new_channel()

        self.echo.log_debug(f'rpc_call :: ch {ch.channel_id}')

        if self._alone(ch, topic, alone):
            return None

        self.echo.log_debug('rpc_call :: start init answer topic')
        answer_topic = self._init_topic(ch, self.config.answer_exchange_name, f'{meta.answer_params.topic}.{meta.answer_params.id}')

        self._publish_ch(ch, self.config.exchange_name, topic, meta, self._mk_mesg_props_by_meta(meta), alone)

        started = time.time()
        self.echo.log_info(f'waiting for message from {self.config.answer_exchange_name}->{answer_topic}')

        delay = 0.1
        while True:  # TODO: rewrite it to consumer
            current_delay = time.time() - started
            self._interact()
            try:
                msg: Optional[amqp.Message] = ch.basic_get(queue=answer_topic, no_ack=True)
            except RECOVERABLE_ERRORS as e:
                self._uni_echo.log_warning(f'error occurred while getting message from answer queue :: {str(e)}')
                sleep(delay)
                continue
            self._interact()

            if msg is None:
                if current_delay > max_delay_s:
                    raise UniAnswerDelayError(f'answer for {self.config.answer_exchange_name}->{answer_topic} reached delay limit. {current_delay}s > {max_delay_s}s')
                self.echo.log_debug(f'no answer {int(time.time() - started + 1)}s in {self.config.answer_exchange_name}->{answer_topic}')
                sleep(delay)
                continue

            self.echo.log_debug(f'got answer from {self.config.answer_exchange_name}->{answer_topic}')

            return self.parse_message_body(
                msg.body,
                compression=(msg.headers or dict()).get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                content_type=msg.content_type,
                unwrapped=unwrapped,
            )

        if not active_ch and ch.is_open:
            self._interact()
            ch.close()
            self._interact()

    def publish_answer(self, answer_params: UniAnswerParams, meta: UniMessageMeta) -> None:
        headers = dict()

        if self.definition.compression is not None:
            headers[BASIC_PROPERTIES__HEADER__COMPRESSION_KEY] = self.definition.compression

        ttl_s = meta.real_ttl_s
        if ttl_s is not None:
            headers[HEADER_TTL] = str(ttl_s * 1000)

        props = UniAmqpPyBrokerMsgProps(
            content_type=self.definition.content_type,
            content_encoding='utf-8',
            delivery_mode=1,
            application_headers=headers,
        )

        self._interact()
        with self._new_channel() as ch:
            self._interact()
            self._publish_ch(ch, self.config.answer_exchange_name, f'{answer_params.topic}.{answer_params.id}', meta, props, alone=False)
