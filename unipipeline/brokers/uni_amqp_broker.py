from typing import Optional, Any, Tuple, TypeVar, Set, List
from urllib.parse import urlparse

from pika import ConnectionParameters, PlainCredentials, BlockingConnection, BasicProperties, spec  # type: ignore
from pika.adapters.blocking_connection import BlockingChannel  # type: ignore
from pydantic import BaseModel

from unipipeline.modules.uni_broker import UniBroker, UniBrokerMessageManager, UniBrokerConsumer
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_codec import UniMessageCodec
from unipipeline.modules.uni_message_meta import UniMessageMeta
from unipipeline.utils.connection_pool import ConnectionObj, TConnectionObj, connection_pool

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


class UniAmqpBrokerConnectionObj(ConnectionObj[BlockingConnection]):

    def __init__(self, params: ConnectionParameters) -> None:
        self._params = params
        self._connection: Optional[BlockingConnection] = None

    def __hash__(self) -> int:
        return hash(f'{self._params.host}{self._params.port}{self._params.credentials.username}{self._params.credentials.password}')

    def get(self) -> TConnectionObj:
        assert self._connection is not None
        return self._connection

    def is_closed(self) -> bool:
        return self._connection is None or self._connection.is_closed

    def connect(self) -> None:
        assert self._connection is None
        self._connection = BlockingConnection(self._params)

    def close(self):
        if self._connection is not None:
            if not self._connection.is_closed:
                self._connection.close()
            self._connection = None


class UniAmqpBrokerConfig(BaseModel):
    exchange_name: str
    heartbeat: int
    blocked_connection_timeout: int
    prefetch: int
    socket_timeout: int
    stack_timeout: int
    exchange_type: str
    durable: bool
    auto_delete: bool
    passive: bool
    is_persistent: bool


class UniAmqpBroker(UniBroker[bytes]):
    def get_topic_approximate_messages_count(self, topic: str) -> int:
        raise NotImplementedError(f"method get_topic_approximate_messages_count must be implemented for class '{type(self).__name__}'")  # TODO

    @classmethod
    def get_connection_uri(cls) -> str:
        raise NotImplementedError(f"cls method get_connection_uri must be implemented for class '{cls.__name__}'")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.conf = UniAmqpBrokerConfig(**self.definition.configure_dynamic(dict(
            exchange_name="communication",
            exchange_type="direct",
            heartbeat=600,
            blocked_connection_timeout=300,
            socket_timeout=300,
            stack_timeout=300,
            durable=True,
            passive=False,
            retry_delay_s=3,
            auto_delete=False,
            is_persistent=True,
        )))

        self._consumers_count = 0

        self._connection_key = self.get_connection_uri()
        url_params_pr = urlparse(url=self._connection_key)

        self._connector = connection_pool.new_manager(UniAmqpBrokerConnectionObj(ConnectionParameters(
            heartbeat=self.conf.heartbeat,
            blocked_connection_timeout=self.conf.blocked_connection_timeout,
            socket_timeout=self.conf.socket_timeout,
            stack_timeout=self.conf.stack_timeout,
            retry_delay=self.definition.retry_delay_s,
            host=url_params_pr.hostname,
            port=url_params_pr.port,
            credentials=PlainCredentials(url_params_pr.username, url_params_pr.password, erase_on_connect=False),
        )))

        self._channel: Optional[BlockingChannel] = None
        self._consuming_started = False

    def initialize(self, topics: Set[str]) -> None:
        ch = self._get_channel()
        ch.exchange_declare(
            exchange=self.conf.exchange_name,
            exchange_type=self.conf.exchange_type,
            passive=self.conf.passive,
            durable=self.conf.durable,
            auto_delete=self.conf.auto_delete,
        )

        ch.basic_qos(prefetch_count=self.conf.prefetch)

        for topic in topics:
            ch.queue_declare(
                queue=topic,
                durable=self.conf.durable,
                auto_delete=self.conf.auto_delete
            )

            ch.queue_bind(queue=topic, exchange=self.conf.exchange_name, routing_key=topic)

    def connect(self) -> None:
        self._connector.connect()

    def close(self) -> None:
        self._connector.close()

    def _get_channel(self) -> BlockingChannel:
        if self._channel is not None:
            return self._channel

        self._channel = self._connector.connect().channel()

        return self._channel

    def add_topic_consumer(self, topic: str, consumer: UniBrokerConsumer) -> None:
        if self._consuming_started:
            raise OverflowError('you cannot add consumer dynamically')

        self._consumers_count += 1

        def consumer_wrapper(channel: BlockingChannel, method_frame: spec.Basic.Deliver, properties: BasicProperties, body: bytes) -> None:
            manager = UniAmqpBrokerMessageManager(channel, method_frame)
            meta = self.parse_body(body, properties)
            consumer.message_handler(meta, manager)

        self._get_channel().basic_consume(
            queue=topic,
            on_message_callback=consumer_wrapper,
            consumer_tag=consumer.id,
        )
        self.echo.log_info(f'added consumer for topic "{topic}" with consumer_tag "{consumer.id}')

    def start_consuming(self) -> None:
        if self._consumers_count == 0:
            self.echo.log_warning('has no consumers to start consuming')
            return
        if self._consuming_started:
            raise OverflowError('you cannot consume twice!')
        self._consuming_started = True

        self.echo.log_info(f'start consuming:: has {self._consumers_count} workers')
        self._get_channel().start_consuming()  # blocking operation

    def serialize_body(self, meta: UniMessageMeta) -> Tuple[bytes, BasicProperties]:
        meta_dumps = self.definition.codec.dumps(meta.dict())
        meta_compressed = self.definition.codec.compress(meta_dumps)

        properties = BasicProperties(
            content_type=self.definition.codec.content_type,
            content_encoding='utf-8',
            delivery_mode=2 if self.conf.is_persistent else 0,
            headers={BASIC_PROPERTIES__HEADER__COMPRESSION_KEY: self.definition.codec.compression}
        )
        return meta_compressed, properties

    def parse_body(self, body: bytes, properties: BasicProperties) -> UniMessageMeta:
        codec: UniMessageCodec[Any] = UniMessageCodec(
            compression=properties.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
            content_type=properties.content_type
        )
        body_uncompressed = codec.decompress(body)
        body_json = codec.loads(body_uncompressed)
        return UniMessageMeta(**body_json)

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        for meta in meta_list:
            body, properties = self.serialize_body(meta)
            self._get_channel().basic_publish(
                exchange=self.conf.exchange_name,
                routing_key=topic,
                body=body,
                properties=properties
            )
