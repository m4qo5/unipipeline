from typing import Callable, Optional, Any, Tuple, TypeVar
from urllib.parse import urlparse

from pika import ConnectionParameters, PlainCredentials, BlockingConnection, BasicProperties, spec
from pika.adapters.blocking_connection import BlockingChannel

from unipipeline.modules.uni_broker import UniBroker, UniBrokerMessageManager
from unipipeline.modules.uni_broker_definition import UniBrokerDefinition, UniMessageCodec
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_meta import UniMessageMeta
from unipipeline.utils.connection_pool import ConnectionObj, TConnectionObj, connection_pool

BASIC_PROPERTIES__HEADER__COMPRESSION_KEY = 'compression'


TMessage = TypeVar('TMessage', bound=UniMessage)


class AmqpUniBrokerMessageManager(UniBrokerMessageManager):

    def __init__(self, channel: BlockingChannel, method_frame: spec.Basic.Deliver) -> None:
        self._channel = channel
        self._method_frame = method_frame
        self._acked = False

    def reject(self) -> None:
        self._channel.basic_reject(delivery_tag=self._method_frame.delivery_tag, requeue=True)

    def ack(self) -> None:
        if self._acked:
            return
        self._acked = True
        self._channel.basic_ack(delivery_tag=self._method_frame.delivery_tag)


class RMQConnectionObj(ConnectionObj[BlockingConnection]):

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


class AmqpUniBroker(UniBroker):
    @classmethod
    def get_connection_uri(cls) -> str:
        raise NotImplementedError(f"cls method get_connection_uri must be implemented for class '{cls.__name__}'")

    def get_exchange_name(self) -> str:
        return self.definition.rmq_definition.exchange_name

    def __init__(self, definition: UniBrokerDefinition[bytes]) -> None:
        super().__init__(definition)

        self._connection_key = self.get_connection_uri()
        url_params_pr = urlparse(url=self._connection_key)

        params = ConnectionParameters(
            heartbeat=self.definition.rmq_definition.heartbeat,
            blocked_connection_timeout=self.definition.rmq_definition.blocked_connection_timeout,
            socket_timeout=self.definition.rmq_definition.socket_timeout,
            stack_timeout=self.definition.rmq_definition.stack_timeout,
            retry_delay=self.definition.retry_delay_s,
            host=url_params_pr.hostname,
            port=url_params_pr.port,
            credentials=PlainCredentials(url_params_pr.username, url_params_pr.password, erase_on_connect=False),
        )

        self._connector = connection_pool.new_manager(RMQConnectionObj(params))

        self._read_channel: Optional[BlockingChannel] = None
        self._write_channel: Optional[BlockingChannel] = None
        self._consuming_started = False

    def connect(self) -> None:
        self._connector.connect()

    def close(self) -> None:
        self._connector.close()

    def _bind(self, is_read: bool, topic: str) -> None:
        if is_read:
            channel = self._read_channel
        else:
            channel = self._write_channel

        if channel is not None:
            return

        channel = self._connector.connect().channel()
        exchange = self.get_exchange_name()
        channel.exchange_declare(
            exchange=exchange,
            exchange_type=self.definition.rmq_definition.exchange_type,
            passive=self.definition.passive,
            durable=self.definition.durable,
            auto_delete=self.definition.auto_delete,
        )

        channel.queue_declare(
            queue=topic,
            durable=self.definition.durable,
            auto_delete=self.definition.auto_delete
        )
        channel.queue_bind(queue=topic, exchange=exchange, routing_key=topic)

        if is_read:
            self._read_channel = channel
        else:
            self._write_channel = channel

    def consume(
        self,
        topic: str,
        processor: Callable[[UniMessageMeta, UniBrokerMessageManager], None],
        consumer_tag: str,
        worker_name: str,
        prefetch: int = 1
    ) -> None:
        if self._consuming_started:
            raise ConnectionError('you cannot consume twice!')
        self._consuming_started = True
        self._bind(True, topic)
        assert self._read_channel is not None
        self._read_channel.basic_qos(prefetch_count=prefetch)
        self._read_channel.basic_consume(
            queue=topic,
            on_message_callback=self._wrap_message(processor),
            consumer_tag=consumer_tag,
        )
        self._read_channel.start_consuming()

    def serialize_body(self, meta: UniMessageMeta) -> Tuple[bytes, BasicProperties]:
        meta_dumps = self.definition.message_codec.dumps(meta.dict())
        meta_compressed = self.definition.message_codec.compress(meta_dumps)

        properties = BasicProperties(
            content_type=self.definition.message_codec.content_type,
            content_encoding='utf-8',
            delivery_mode=2 if self.definition.is_persistent else 0,
            headers={BASIC_PROPERTIES__HEADER__COMPRESSION_KEY: self.definition.message_codec.compression}
        )
        return meta_compressed, properties

    def parse_body(self, body: bytes, properties: BasicProperties) -> UniMessageMeta:
        codec: UniMessageCodec[Any] = UniMessageCodec(
            compression=properties.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
            encoding=properties.content_encoding,
            content_type=properties.content_type
        )
        body_uncompressed = codec.decompress(body)
        body_json = codec.loads(body_uncompressed)
        return UniMessageMeta(**body_json)

    def _wrap_message(self, processor: Callable[[UniMessageMeta, UniBrokerMessageManager], None]) -> Callable[[BlockingChannel, Any, Any, bytes], None]:
        def wrapper(channel: BlockingChannel, method_frame: spec.Basic.Deliver, properties: BasicProperties, body: bytes) -> None:
            manager = AmqpUniBrokerMessageManager(channel, method_frame)
            meta = self.parse_body(body, properties)
            processor(meta, manager)

        return wrapper

    def publish(self, topic: str, meta: UniMessageMeta) -> None:
        self._bind(False, topic)

        body, properties = self.serialize_body(meta)

        assert self._write_channel is not None
        self._write_channel.basic_publish(
            exchange=self.get_exchange_name(),
            routing_key=topic,
            body=body,
            properties=properties
        )
