import time
from time import sleep
from typing import Optional, TypeVar, Set, List, NamedTuple, Callable, TYPE_CHECKING
from urllib.parse import urlparse

from pika import ConnectionParameters, PlainCredentials, BlockingConnection, BasicProperties, spec  # type: ignore
from pika.adapters.blocking_connection import BlockingChannel  # type: ignore
from pika.exceptions import AMQPConnectionError, AMQPError, ConnectionClosedByBroker  # type: ignore

from unipipeline.brokers.uni_broker_consumer import UniBrokerConsumer
from unipipeline.errors.uni_answer_delay_error import UniAnswerDelayError
from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.brokers.uni_broker_message_manager import UniBrokerMessageManager
from unipipeline.definitions.uni_definition import UniDynamicDefinition
from unipipeline.message.uni_message import UniMessage
from unipipeline.message_meta.uni_message_meta import UniMessageMeta, UniAnswerParams

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator

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
    answer_exchange_name: str = "communication_answer"
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


class UniAmqpBroker(UniBroker[UniAmqpBrokerConfig]):
    config_type = UniAmqpBrokerConfig

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        res = self._get_channel().queue_declare(queue=topic, passive=True)
        return int(res.method.message_count)

    @classmethod
    def get_connection_uri(cls) -> str:
        raise NotImplementedError(f"cls method get_connection_uri must be implemented for class '{cls.__name__}'")

    def __init__(self, mediator: 'UniMediator', definition: UniBrokerDefinition) -> None:
        super().__init__(mediator, definition)

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

    def initialize(self, topics: Set[str], answer_topic: Set[str]) -> None:
        ch = self._get_channel()
        ch.exchange_declare(
            exchange=self.config.exchange_name,
            exchange_type=self.config.exchange_type,
            passive=self.config.passive,
            durable=self.config.durable,
            auto_delete=self.config.auto_delete,
        )
        ch.exchange_declare(
            exchange=self.config.answer_exchange_name,
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

    def _end_consuming(self) -> None:
        if not self._consuming_started:
            return
        self._interrupted = True
        if not self._in_processing:
            self._get_channel().stop_consuming()
            self.close()
            self._consuming_started = False
            self.echo.log_info('consumption stopped')

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

        self.echo.log_info('connected')

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

    def _get_channel(self, new: bool = False) -> BlockingChannel:
        self.connect()
        if new:
            assert self._connection is not None
            return self._connection.channel()
        assert self._channel is not None
        return self._channel

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        echo = self.echo.mk_child(f'topic[{consumer.topic}]')
        if self._consuming_started:
            echo.exit_with_error(f'you cannot add consumer dynamically :: tag="{consumer.id}" group_id={consumer.group_id}')

        def consumer_wrapper(channel: BlockingChannel, method_frame: spec.Basic.Deliver, properties: BasicProperties, body: bytes) -> None:
            self._in_processing = True

            meta = self.parse_message_body(
                body,
                compression=properties.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                content_type=properties.content_type,
                unwrapped=consumer.unwrapped,
            )

            manager = UniAmqpBrokerMessageManager(channel, method_frame)
            consumer.message_handler(meta, manager)
            self._in_processing = False
            if self._interrupted:
                self._end_consuming()

        self._consumers.append(UniAmqpBrokerConsumer(
            queue=consumer.topic,
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
                    echo.log_debug(f'added consumer {c.consumer_tag} of {c.queue}')
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
        ch = self._get_channel()
        for meta in meta_list:  # TODO: package sending
            # TODO: retry
            headers = {
                BASIC_PROPERTIES__HEADER__COMPRESSION_KEY: self.definition.compression,
                # **({'x-message-ttl': ttl_s * 1000} if ttl_s is not None else {}),
            }
            if meta.need_answer:
                assert meta.answer_params is not None
                props = BasicProperties(
                    content_type=self.definition.content_type,
                    content_encoding='utf-8',
                    reply_to=f'{meta.answer_params.topic}.{meta.answer_params.id}',
                    correlation_id=str(meta.id),
                    delivery_mode=2 if self.config.is_persistent else 0,
                    headers=headers
                )
            else:
                props = BasicProperties(
                    content_type=self.definition.content_type,
                    content_encoding='utf-8',
                    delivery_mode=2 if self.config.is_persistent else 0,
                    headers=headers
                )
            ch.basic_publish(
                exchange=self.config.exchange_name,
                routing_key=topic,
                body=self.serialize_message_body(meta),
                properties=props
            )
        self.echo.log_debug(f'sent messages ({len(meta_list)}) to {self.config.exchange_name}->{topic}')

    def get_answer(self, answer_params: UniAnswerParams, max_delay_s: int, unwrapped: bool) -> UniMessageMeta:
        answ_topic = f'{answer_params.topic}.{answer_params.id}'
        exchange = self.config.answer_exchange_name
        ch = self._get_channel(True)

        ch.queue_declare(queue=answ_topic, durable=False, exclusive=True, passive=False)

        ch.queue_bind(queue=answ_topic, exchange=exchange, routing_key=answ_topic)

        started = time.time()
        while True:
            (method, properties, body) = ch.basic_get(queue=answ_topic, auto_ack=True)

            if method is None:
                if (time.time() - started) > max_delay_s:
                    raise UniAnswerDelayError(f'answer for {exchange}->{answ_topic} reached delay limit {max_delay_s} seconds')
                self.echo.log_debug(f'no answer {int(time.time() - started + 1)}s in {exchange}->{answ_topic}')
                sleep(1)
                continue

            self.echo.log_debug(f'took answer from {exchange}->{answ_topic}')
            return self.parse_message_body(
                body,
                compression=properties.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                content_type=properties.content_type,
                unwrapped=unwrapped,
            )

    def publish_answer(self, answer_params: UniAnswerParams, meta: UniMessageMeta) -> None:
        ch = self._get_channel()
        answ_topic = f'{answer_params.topic}.{answer_params.id}'
        ch.basic_publish(
            exchange=self.config.answer_exchange_name,
            routing_key=answ_topic,
            body=self.serialize_message_body(meta),
            properties=BasicProperties(
                content_type=self.definition.content_type,
                content_encoding='utf-8',
                delivery_mode=1,
                headers={
                    BASIC_PROPERTIES__HEADER__COMPRESSION_KEY: self.definition.compression,
                    # **({'x-message-ttl': ttl_s * 1000} if ttl_s is not None else {}),
                }
            )
        )
        self.echo.log_debug(f'sent message to {self.config.answer_exchange_name}->{answ_topic}')
