import functools
import logging
import time
import urllib.parse
from time import sleep
from typing import Optional, TypeVar, Set, List, NamedTuple, Callable, TYPE_CHECKING, Dict
from urllib.parse import urlparse

import amqp  # type: ignore
from amqp.exceptions import ConnectionError as AqmpConnectionError, RecoverableChannelError, AMQPError  # type: ignore

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.brokers.uni_broker_consumer import UniBrokerConsumer
from unipipeline.brokers.uni_broker_message_manager import UniBrokerMessageManager
from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.definitions.uni_definition import UniDynamicDefinition
from unipipeline.errors.uni_answer_delay_error import UniAnswerDelayError
from unipipeline.message.uni_message import UniMessage
from unipipeline.message_meta.uni_message_meta import UniMessageMeta, UniAnswerParams
from unipipeline.utils.uni_echo import UniEcho

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator

BASIC_PROPERTIES__HEADER__COMPRESSION_KEY = 'compression'

RECOVERABLE_ERRORS = (AqmpConnectionError, RecoverableChannelError)


TMessage = TypeVar('TMessage', bound=UniMessage)

logging.getLogger('amqp').setLevel(logging.DEBUG)

T = TypeVar('T')


class UniPyPikaBrokerMessageManager(UniBrokerMessageManager):

    def __init__(self, channel: amqp.Channel, delivery_tag: str) -> None:
        self._channel = channel
        self._delivery_tag = delivery_tag
        self._acknowledged = False

    def reject(self) -> None:
        self._channel.basic_reject(delivery_tag=self._delivery_tag, requeue=True)

    def ack(self) -> None:
        if self._acknowledged:
            return
        self._acknowledged = True
        self._channel.basic_ack(delivery_tag=self._delivery_tag)


class UniAmqpPyBrokerConfig(UniDynamicDefinition):
    exchange_name: str = "communication"
    answer_exchange_name: str = "communication_answer"
    heartbeat: int = 600
    blocked_connection_timeout: int = 300
    prefetch: int = 1
    retry_max_count: int = 100
    retry_delay_s: int = 3
    socket_timeout: int = 300
    stack_timeout: int = 400
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
    queue: str
    on_message_callback: Callable[[amqp.Channel, 'amqp.Message'], None]
    consumer_tag: str
    prefetch_count: int


class AmqpPyChannelObj:
    def __init__(self, name: str, ttl: int, broker: 'UniAmqpPyBroker') -> None:
        self._name = name
        self._broker = broker
        self._ch: Optional[amqp.Channel] = None
        self._ttl = ttl
        self._ch_time: float = 0

    def close_channel(self) -> None:
        if self._ch is None or not self._ch.is_open:
            return
        self._ch.close()
        self._broker.echo.log_info(f'channel {self._name} closed')

    def get_channel(self, *, force_recreate: bool = False, force_current: bool = False) -> amqp.Channel:
        if force_recreate and not force_current:
            self.close_channel()
            return self.get_channel(force_recreate=False)

        if self._ch is None or not self._ch.is_open:
            self._ch = self._broker.connected_connection.channel()
            self._ch_time = time.time()
            self._broker.echo.log_info(f'channel {self._name} established')
        else:
            if not force_current and (time.time() - self._ch_time > self._ttl):
                return self.get_channel(force_recreate=True)
            self._broker.echo.log_debug(f'channel {self._name} reused')
        return self._ch


class UniAmqpPyBroker(UniBroker[UniAmqpPyBrokerConfig]):
    config_type = UniAmqpPyBrokerConfig

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        res = self._ch_stat.get_channel().queue_declare(queue=topic, passive=True)
        return int(res.method.message_count)

    @classmethod
    def get_connection_uri(cls) -> str:
        raise NotImplementedError(f"cls method get_connection_uri must be implemented for class '{cls.__name__}'")

    @functools.cached_property
    def parsed_connection_uri(self) -> urllib.parse.ParseResult:
        return urlparse(url=self.get_connection_uri())

    def __init__(self, mediator: 'UniMediator', definition: UniBrokerDefinition) -> None:
        super().__init__(mediator, definition)

        self._consumers: List[UniAmqpPyBrokerConsumer] = list()

        self._connection: Optional[amqp.Connection] = None

        self._ch_initializer = AmqpPyChannelObj('initializer', int(self.config.heartbeat / 2), self)
        self._ch_stat = AmqpPyChannelObj('stat', int(self.config.heartbeat / 2), self)
        self._ch_publisher = AmqpPyChannelObj('publisher', int(self.config.heartbeat / 2), self)
        self._ch_answ_publisher = AmqpPyChannelObj('answer_publisher', int(self.config.heartbeat / 2), self)
        self._ch_consumer = AmqpPyChannelObj('consumer', int(self.config.heartbeat / 2), self)
        self._ch_answ_consumer = AmqpPyChannelObj('answer_consumer', int(self.config.heartbeat / 2), self)

        self._consuming_enabled = False
        self._in_processing = False
        self._interrupted = False

        self._initialized_exchanges: Set[str] = set()
        self._initialized_topics: Set[str] = set()

    @property
    def connected_connection(self) -> amqp.Connection:
        self.connect()
        assert self._connection is not None
        return self._connection

    def _init_exchange(self, ch: amqp.Channel, exchange: str) -> None:
        if exchange in self._initialized_topics:
            return
        self._initialized_exchanges.add(exchange)
        ch.exchange_declare(
            exchange=exchange,
            type="direct",
            passive=False,
            durable=True,
            auto_delete=False,
        )
        self.echo.log_info(f'exchange "{exchange}" initialized')

    def _init_topic(self, ch: amqp.Channel, exchange: str, topic: str) -> str:
        q_key = f'{exchange}->{topic}'
        if q_key in self._initialized_topics:
            return topic
        self._initialized_topics.add(q_key)

        self._init_exchange(ch, exchange)

        if exchange == self.config.exchange_name:
            ch.queue_declare(queue=topic, durable=True, auto_delete=False, passive=False)
        elif exchange == self.config.answer_exchange_name:
            ch.queue_declare(queue=topic, durable=False, auto_delete=True, exclusive=True, passive=False)
        else:
            raise TypeError(f'invalid exchange name "{exchange}"')

        ch.queue_bind(queue=topic, exchange=self.config.exchange_name, routing_key=topic)
        self.echo.log_info(f'queue "{q_key}" initialized')
        return topic

    def initialize(self, topics: Set[str], answer_topic: Set[str]) -> None:
        return

    def stop_consuming(self) -> None:
        self._end_consuming()

    def _end_consuming(self) -> None:
        if not self._consuming_enabled:
            return
        self._interrupted = True
        if not self._in_processing:
            # self.close()
            self._consuming_enabled = False
            self.echo.log_info('consumption stopped')

    def connect(self) -> None:
        try:
            if self._connection is None or not self._connection.connected:
                self._connection = amqp.Connection(
                    host=f'{self.parsed_connection_uri.hostname}:{self.parsed_connection_uri.port}',
                    password=self.parsed_connection_uri.password,
                    userid=self.parsed_connection_uri.username,
                    heartbeat=self.config.heartbeat,
                )
                self._connection.connect()
                self.echo.log_info('connected')
        except RECOVERABLE_ERRORS as e:
            raise ConnectionError(str(e))

    def close(self) -> None:
        if self._connection is None:
            return

        if not self._connection.connected:
            self._connection = None
            return

        try:
            self._connection.close()
            self._connection = None
        except AMQPError:
            pass

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        echo = self.echo.mk_child(f'topic[{consumer.topic}]')
        if self._consuming_enabled:
            echo.exit_with_error(f'you cannot add consumer dynamically :: tag="{consumer.id}" group_id={consumer.group_id}')

        def consumer_wrapper(ch: amqp.Channel, message: amqp.Message) -> None:
            self._in_processing = True

            meta = self.parse_message_body(
                message.body,
                compression=message.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                content_type=message.content_type,
                unwrapped=consumer.unwrapped,
            )

            manager = UniPyPikaBrokerMessageManager(ch, message.delivery_tag)
            consumer.message_handler(meta, manager)

            self._in_processing = False
            if self._interrupted:
                self._end_consuming()

        self._consumers.append(UniAmqpPyBrokerConsumer(
            queue=consumer.topic,
            on_message_callback=consumer_wrapper,
            consumer_tag=consumer.id,
            prefetch_count=consumer.prefetch_count,
        ))

        echo.log_info(f'added consumer :: tag="{consumer.id}" group_id={consumer.group_id}')

    def _start_consuming(self) -> None:
        echo = self.echo.mk_child('consuming')
        if len(self._consumers) == 0:
            echo.log_warning('has no consumers to start consuming')
            return
        conn = self.connected_connection

        for c in self._consumers:
            ch = conn.channel()
            topic = self._init_topic(ch, self.config.exchange_name, c.queue)
            ch.basic_consume(queue=topic, callback=functools.partial(c.on_message_callback, ch), consumer_tag=c.consumer_tag)
            ch.basic_qos(prefetch_count=self.config.prefetch, a_global=False, prefetch_size=0)
            echo.log_debug(f'added consumer {c.consumer_tag} on {self.config.exchange_name}->{topic}')

        echo.log_info(f'starting consuming :: consumers_count={len(self._consumers)}')

        while self._consuming_enabled:  # blocking operation
            echo.log_debug('wait for next message ...')
            conn.drain_events()

    def _retry_run(self, echo: UniEcho, fn: Callable[[], T]) -> T:
        retry_counter = 0
        max_retries = max(self.config.retry_max_count, 1)
        retry_threshold_s = self.config.retry_delay_s * max_retries
        while True:
            start = time.time()
            try:
                return fn()
            except RECOVERABLE_ERRORS as e:
                echo.log_error(f'connection closed {e}')
                if int(time.time() - start) >= retry_threshold_s:
                    retry_counter = 0
                if retry_counter >= max_retries:
                    raise ConnectionError()
                retry_counter += 1
                sleep(self.config.retry_delay_s)
                echo.log_warning(f'retry {retry_counter}/{max_retries} :: {e}')
            except AMQPError as e:
                echo.log_warning(f"Caught a channel error: {e}, stopping...")
                raise

    def start_consuming(self) -> None:
        echo = self.echo.mk_child('consuming')
        if self._consuming_enabled:
            echo.log_warning('consuming has already started. ignored')
            return
        self._consuming_enabled = True
        self._interrupted = False
        self._in_processing = False

        self._retry_run(echo, self._start_consuming)

    def _publish(self, ch: amqp.Channel, exchange: str, topic: str, meta: UniMessageMeta, props: UniAmqpPyBrokerMsgProps) -> None:
        self.echo.log_debug(f'message start publishing to {exchange}->{topic}')
        topic = self._init_topic(ch, exchange, topic)
        ch.basic_publish(
            amqp.Message(
                body=self.serialize_message_body(meta),
                **props._asdict(),
            ),
            exchange=exchange,
            routing_key=topic,
            mandatory=self.config.mandatory_publishing,
            # immediate=self.config.immediate_publishing,
        )
        self.echo.log_debug(f'message published to {exchange}->{topic}')

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        ch = self._ch_publisher.get_channel()
        echo = self.echo.mk_child('publish')
        for meta in meta_list:  # TODO: package sending
            headers = dict()
            if self.definition.compression is not None:
                headers[BASIC_PROPERTIES__HEADER__COMPRESSION_KEY] = self.definition.compression
            if meta.ttl_s:
                headers['x-message-ttl'] = str(meta.ttl_s * 1000)

            if meta.need_answer:
                assert meta.answer_params is not None
                props = UniAmqpPyBrokerMsgProps(
                    content_type=self.definition.content_type,
                    content_encoding='utf-8',
                    reply_to=f'{meta.answer_params.topic}.{meta.answer_params.id}',
                    correlation_id=str(meta.id),
                    delivery_mode=2 if self.config.persistent_message else 0,
                    application_headers=headers,
                )
            else:
                props = UniAmqpPyBrokerMsgProps(
                    content_type=self.definition.content_type,
                    content_encoding='utf-8',
                    delivery_mode=2 if self.config.persistent_message else 0,
                    application_headers=headers,
                )
            self._retry_run(echo, functools.partial(self._publish, ch=ch, exchange=self.config.exchange_name, topic=topic, meta=meta, props=props))
        self.echo.log_info(f'{list(meta_list)} messages published to {self.config.exchange_name}->{topic}')

    def _get_answ(self, answer_params: UniAnswerParams, max_delay_s: int, unwrapped: bool) -> UniMessageMeta:
        ch = self._ch_answ_consumer.get_channel(force_recreate=True)
        topic = self._init_topic(ch, self.config.answer_exchange_name, f'{answer_params.topic}.{answer_params.id}')

        started = time.time()
        while True:
            (method, properties, body) = ch.basic_get(queue=topic, no_ack=True)

            if method is None:
                if (time.time() - started) > max_delay_s:
                    raise UniAnswerDelayError(f'answer for {self.config.answer_exchange_name}->{topic} reached delay limit {max_delay_s} seconds')
                self.echo.log_debug(f'no answer {int(time.time() - started + 1)}s in {self.config.answer_exchange_name}->{topic}')
                sleep(1)
                continue

            self.echo.log_debug(f'took answer from {self.config.answer_exchange_name}->{topic}')
            return self.parse_message_body(
                body,
                compression=properties.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                content_type=properties.content_type,
                unwrapped=unwrapped,
            )

    def get_answer(self, answer_params: UniAnswerParams, max_delay_s: int, unwrapped: bool) -> UniMessageMeta:
        echo = self.echo.mk_child('get_answer')
        return self._retry_run(echo, functools.partial(self._get_answ, answer_params=answer_params, max_delay_s=max_delay_s, unwrapped=unwrapped))

    def publish_answer(self, answer_params: UniAnswerParams, meta: UniMessageMeta) -> None:
        echo = self.echo.mk_child('publish_answer')
        props = UniAmqpPyBrokerMsgProps(
            content_type=self.definition.content_type,
            content_encoding='utf-8',
            delivery_mode=1,
        )
        ch = self._ch_answ_publisher.get_channel()
        self._retry_run(echo, functools.partial(self._publish, ch=ch, exchange=self.config.answer_exchange_name, topic=f'{answer_params.topic}.{answer_params.id}', meta=meta, props=props))
