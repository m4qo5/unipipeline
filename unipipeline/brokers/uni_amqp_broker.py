import time
from time import sleep
from typing import Optional, TypeVar, Set, List, NamedTuple, Callable
from urllib.parse import urlparse

from pika import ConnectionParameters, PlainCredentials, BlockingConnection, BasicProperties, spec  # type: ignore
from pika.adapters.blocking_connection import BlockingChannel  # type: ignore
from pika.exceptions import AMQPConnectionError, AMQPError, ConnectionClosedByBroker  # type: ignore

from unipipeline.modules.uni_broker import UniBroker, UniBrokerMessageManager, UniBrokerConsumer
from unipipeline.modules.uni_definition import UniDynamicDefinition
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_codec import UniMessageCodec
from unipipeline.modules.uni_message_meta import UniMessageMeta

BASIC_PROPERTIES__HEADER__COMPRESSION_KEY = 'compression'


TMessage = TypeVar('TMessage', bound=UniMessage)


class UniAmqpBrokerMessageManager(UniBrokerMessageManager):

    def __init__(self, channel: BlockingChannel, method_frame: spec.Basic.Deliver) -> None:
        self._channel = channel
        self._method_frame = method_frame
        self._acknowledged = False

    def reject(self) -> None:
        self._channel.basic_reject(delivery_tag=self._method_frame.delivery_tag, requeue=True)

    def ack(self) -> None:
        if self._acknowledged:
            return
        self._acknowledged = True
        self._channel.basic_ack(delivery_tag=self._method_frame.delivery_tag)


class UniAmqpBrokerConfig(UniDynamicDefinition):
    exchange_name: str = "communication"
    heartbeat: int = 600
    blocked_connection_timeout: int = 300
    prefetch: int = 1
    retry_max_count: int = 100
    retry_delay_s: int = 3
    socket_timeout: int = 300
    stack_timeout: int = 300
    exchange_type: str = "direct"
    durable: bool = True
    auto_delete: bool = False
    passive: bool = False
    is_persistent: bool = True


class UniAmqpBrokerConsumer(NamedTuple):
    queue: str
    on_message_callback: Callable[[BlockingChannel, spec.Basic.Deliver, BasicProperties, bytes], None]
    consumer_tag: str


class UniAmqpBroker(UniBroker[bytes, UniAmqpBrokerConfig]):
    config_type = UniAmqpBrokerConfig

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        ch = self._get_channel()
        res = ch.queue_declare(
            queue=topic,
            passive=True
        )
        return res.method.message_count

    @classmethod
    def get_connection_uri(cls) -> str:
        raise NotImplementedError(f"cls method get_connection_uri must be implemented for class '{cls.__name__}'")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        broker_url = self.get_connection_uri()

        url_params_pr = urlparse(url=broker_url)

        self._params = ConnectionParameters(
            heartbeat=self.config.heartbeat,
            blocked_connection_timeout=self.config.blocked_connection_timeout,
            socket_timeout=self.config.socket_timeout,
            stack_timeout=self.config.stack_timeout,
            retry_delay=self.definition.retry_delay_s,
            host=url_params_pr.hostname,
            port=url_params_pr.port,
            credentials=PlainCredentials(url_params_pr.username, url_params_pr.password, erase_on_connect=False),
        )

        self._consumers: List[UniAmqpBrokerConsumer] = list()

        self._connection: Optional[BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None

        self._consuming_started = False
        self._in_processing = False
        self._interrupted = False

    def initialize(self, topics: Set[str]) -> None:
        ch = self._get_channel()
        ch.exchange_declare(
            exchange=self.config.exchange_name,
            exchange_type=self.config.exchange_type,
            passive=self.config.passive,
            durable=self.config.durable,
            auto_delete=self.config.auto_delete,
        )

        ch.basic_qos(prefetch_count=self.config.prefetch)

        for topic in topics:
            ch.queue_declare(
                queue=topic,
                durable=self.config.durable,
                auto_delete=self.config.auto_delete,
                passive=False,
            )

            ch.queue_bind(queue=topic, exchange=self.config.exchange_name, routing_key=topic)

    def stop_consuming(self) -> None:
        self._end_consuming()

    def _end_consuming(self):
        if not self._consuming_started:
            return
        self._interrupted = True
        if not self._in_processing:
            self._get_channel().stop_consuming()
            self.close()
            self._consuming_started = False

    def connect(self) -> None:
        if self._connection is not None:
            if self._connection.is_closed:
                self._connection = None
            else:
                return
        if self._channel is not None:
            if self._channel.is_closed:
                self._channel = None
            else:
                return
        try:
            self._connection = BlockingConnection(self._params)
            self._channel = self._connection.channel()
        except (AMQPError, AMQPConnectionError) as e:
            raise ConnectionError(str(e))

    def close(self) -> None:
        try:
            if self._channel is not None and not self._channel.is_closed:
                self._channel.close()
        except AMQPError:
            pass

        try:
            if self._connection is not None and not self._connection.is_closed:
                self._connection.close()
        except AMQPError:
            pass

        self._connection = None
        self._channel = None

    def _get_channel(self) -> BlockingChannel:
        self.connect()
        assert self._channel is not None
        return self._channel

    def add_topic_consumer(self, topic: str, consumer: UniBrokerConsumer) -> None:
        echo = self.echo.mk_child(f'topic[{topic}]')
        if self._consuming_started:
            echo.log_error(f'you cannot add consumer dynamically :: tag="{consumer.id}" group_id={consumer.group_id}')
            exit(1)

        self.get_topic_approximate_messages_count(topic)

        def consumer_wrapper(channel: BlockingChannel, method_frame: spec.Basic.Deliver, properties: BasicProperties, body: bytes) -> None:
            self._in_processing = True
            meta = self.codec_parse(body, UniMessageCodec(
                compression=properties.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                content_type=properties.content_type
            ))
            manager = UniAmqpBrokerMessageManager(channel, method_frame)
            consumer.message_handler(meta, manager)
            self._in_processing = False
            if self._interrupted:
                self._end_consuming()

        self._consumers.append(UniAmqpBrokerConsumer(
            queue=topic,
            on_message_callback=consumer_wrapper,
            consumer_tag=consumer.id,
        ))

        echo.log_info(f'added consumer :: tag="{consumer.id}" group_id={consumer.group_id}')

    def start_consuming(self) -> None:
        echo = self.echo.mk_child('consuming')
        if len(self._consumers) == 0:
            echo.log_warning('has no consumers to start consuming')
            return
        if self._consuming_started:
            echo.log_warning('consuming has already started. ignored')
            return
        self._consuming_started = True
        self._interrupted = False
        self._in_processing = False

        retry_counter = 0
        retry_threshold_s = self.config.retry_delay_s * (self.config.retry_max_count + 1)
        while True:
            start = time.time()
            try:
                ch = self._get_channel()
                for c in self._consumers:
                    ch.basic_consume(queue=c.queue, on_message_callback=c.on_message_callback, consumer_tag=c.consumer_tag)
                echo.log_info(f'consumers count is {len(self._consumers)}')
                ch.start_consuming()  # blocking operation
            except (ConnectionClosedByBroker, ConnectionError) as e:
                end = time.time()
                echo.log_error(f'connection closed {e}')
                if int(end - start) >= retry_threshold_s:
                    retry_counter = 0
                if retry_counter >= self.config.retry_max_count:
                    raise ConnectionError()
                retry_counter += 1
                sleep(self.config.retry_delay_s)

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        for meta in meta_list:
            self._get_channel().basic_publish(
                exchange=self.config.exchange_name,
                routing_key=topic,
                body=self.codec_serialize(meta),
                properties=BasicProperties(
                    content_type=self.definition.codec.content_type,
                    content_encoding='utf-8',
                    delivery_mode=2 if self.config.is_persistent else 0,
                    headers={
                        BASIC_PROPERTIES__HEADER__COMPRESSION_KEY: self.definition.codec.compression
                    }
                )
            )
