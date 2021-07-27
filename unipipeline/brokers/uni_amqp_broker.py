import asyncio
import time
from typing import Optional, TypeVar, Set, List, NamedTuple, Callable, TYPE_CHECKING, Awaitable, Dict, Any, Tuple

from aio_pika import connect_robust, IncomingMessage, Channel, Queue, Exchange, Message, DeliveryMode, Connection

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.brokers.uni_broker_consumer import UniBrokerConsumer
from unipipeline.brokers.uni_broker_message_manager import UniBrokerMessageManager
from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.definitions.uni_definition import UniDynamicDefinition
from unipipeline.errors.uni_answer_delay_error import UniAnswerDelayError
from unipipeline.message.uni_message import UniMessage
from unipipeline.message_meta.uni_message_meta import UniMessageMeta, UniAnswerParams

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator

BASIC_PROPERTIES__HEADER__COMPRESSION_KEY = 'compression'


TMessage = TypeVar('TMessage', bound=UniMessage)


class UniAmqpBrokerMessageManager(UniBrokerMessageManager):

    def __init__(self, incoming_message: IncomingMessage) -> None:
        self._incoming_message = incoming_message

    async def reject(self) -> None:
        self._incoming_message.reject(requeue=True)

    async def ack(self) -> None:
        self._incoming_message.ack()


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
    on_message_callback: Callable[[IncomingMessage], Awaitable[None]]
    consumer_tag: str


class UniAmqpBroker(UniBroker[UniAmqpBrokerConfig]):
    config_type = UniAmqpBrokerConfig

    async def get_topic_approximate_messages_count(self, topic: str) -> int:
        ch = await self._get_channel()
        res: Queue = await ch.declare_queue(name=topic, passive=True)
        return int(res.declaration_result.message_count)

    @classmethod
    def get_connection_ssl(cls) -> Tuple[bool, Dict[str, Any]]:
        return False, dict()

    @classmethod
    def get_connection_uri(cls) -> str:
        raise NotImplementedError(f"cls method get_connection_uri must be implemented for class '{cls.__name__}'")

    def __init__(self, mediator: 'UniMediator', definition: UniBrokerDefinition, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(mediator, definition, loop)

        self._consumers: List[UniBrokerConsumer] = list()

        self._connection: Optional[Connection] = None
        self._channel: Optional[Channel] = None

        self._consuming_started = False
        self._in_processing = False
        self._interrupted = False

        self._default_exchange: Optional[Exchange] = None
        self._answer_exchange: Optional[Exchange] = None

    async def initialize(self, topics: Set[str], answer_topic: Set[str]) -> None:
        ch = await self._get_channel()

        self._default_exchange = await ch.declare_exchange(
            name=self.config.exchange_name,
            type=self.config.exchange_type,
            passive=self.config.passive,
            durable=self.config.durable,
            auto_delete=self.config.auto_delete,
        )

        self._answer_exchange = await ch.declare_exchange(
            name=self.config.answer_exchange_name,
            type=self.config.exchange_type,
            passive=self.config.passive,
            durable=self.config.durable,
            auto_delete=self.config.auto_delete,
        )

        await ch.set_qos(prefetch_count=self.config.prefetch)

        for topic in topics:
            q = await ch.declare_queue(
                name=topic,
                durable=self.config.durable,
                auto_delete=self.config.auto_delete,
                passive=False,
            )

            await q.bind(exchange=self.config.exchange_name, routing_key=topic)

    async def stop_consuming(self) -> None:
        await self._end_consuming()

    async def _end_consuming(self) -> None:
        if not self._consuming_started:
            return
        self._interrupted = True
        if not self._in_processing:
            # ch = await self._get_channel()
            # ch.stop_consuming()
            await self.close()
            self._consuming_started = False
            self.echo.log_info('consumption stopped')

    async def connect(self) -> None:
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

        ssl, ssl_options = self.get_connection_ssl()
        self._connection = await connect_robust(
            self.get_connection_uri(),
            ssl=ssl,
            ssl_options=ssl_options,
            loop=self.loop
        )

        assert self._connection is not None
        self._channel = await self._connection.channel()
        self.echo.log_info('connected')

    async def close(self) -> None:
        if self._channel is not None and not self._channel.is_closed:
            self._channel.close()
        if self._connection is not None and not self._connection.is_closed:
            self._connection.close()
        self._connection = None
        self._channel = None

    async def _get_channel(self, new: bool = False) -> Channel:
        await self.connect()
        if new:
            assert self._connection is not None
            return await self._connection.channel()
        assert self._channel is not None
        return self._channel

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        echo = self.echo.mk_child(f'topic[{consumer.topic}]')
        if self._consuming_started:
            echo.exit_with_error(f'you cannot add consumer dynamically :: tag="{consumer.id}" group_id={consumer.group_id}')

        self._consumers.append(consumer)

        echo.log_info(f'added consumer :: tag="{consumer.id}" group_id={consumer.group_id}')

    async def start_consuming(self) -> None:
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

        ch = await self._get_channel()

        await ch.set_qos(prefetch_count=self.config.prefetch)

        consume_futures = list()
        for c in self._consumers:
            q = await ch.get_queue(c.topic, ensure=True)

            async def consumer_wrapper(im: IncomingMessage) -> None:
                self._in_processing = True
                async with im.process(requeue=True):
                    meta = await self.parse_message_body(im.body, im.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None), im.content_type, c.unwrapped)
                    manager = UniAmqpBrokerMessageManager(im)
                    await c.message_handler(meta, manager)
                self._in_processing = False
                if self._interrupted:
                    await self._end_consuming()

            f = q.consume(consumer_wrapper, consumer_tag=c.id)
            consume_futures.append(f)

        await asyncio.gather(*consume_futures)

    async def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        for meta in meta_list:  # TODO: package sending
            answ_options = dict(reply_to=f'{meta.answer_params.topic}.{meta.answer_params.id}', correlation_id=str(meta.id)) if meta.need_answer else dict()  # type: ignore

            message = Message(
                body=await self.serialize_message_body(meta),
                headers=self._get_headers(),
                content_type=self.definition.content_type,
                content_encoding='utf-8',
                delivery_mode=DeliveryMode.PERSISTENT if self.config.is_persistent else DeliveryMode.NOT_PERSISTENT,
                **answ_options,  # type: ignore
            )

            assert self._default_exchange is not None
            await self._default_exchange.publish(message, routing_key=topic)

        self.echo.log_debug(f'sent messages ({len(meta_list)}) to {self.config.exchange_name}->{topic}')

    async def get_answer(self, answer_params: UniAnswerParams, max_delay_s: int, unwrapped: bool) -> UniMessageMeta:
        answ_topic = f'{answer_params.topic}.{answer_params.id}'
        exchange = self.config.answer_exchange_name

        ch = await self._get_channel(True)

        q = await ch.declare_queue(name=answ_topic, durable=False, auto_delete=True, passive=False)

        await q.bind(exchange=exchange, routing_key=answ_topic)

        started = time.time()
        while True:
            im = await q.get(timeout=max_delay_s, fail=False)
            if (time.time() - started) > max_delay_s:
                raise UniAnswerDelayError(f'answer for {exchange}->{answ_topic} reached delay limit {max_delay_s} seconds')
            self.echo.log_debug(f'no answer {int(time.time() - started + 1)}s in {exchange}->{answ_topic}')
            await asyncio.sleep(1)
            continue

        async with im.process():
            self.echo.log_debug(f'took answer from {exchange}->{answ_topic}')
            return await self.parse_message_body(
                im.body,
                compression=im.headers.get(BASIC_PROPERTIES__HEADER__COMPRESSION_KEY, None),
                content_type=im.content_type,
                unwrapped=unwrapped,
            )

    def _get_headers(self) -> Dict[str, str]:
        if self.definition.compression is not None:
            return {
                BASIC_PROPERTIES__HEADER__COMPRESSION_KEY: self.definition.compression,
                # **({'x-message-ttl': ttl_s * 1000} if ttl_s is not None else {}),
            }
        return dict()

    async def publish_answer(self, answer_params: UniAnswerParams, meta: UniMessageMeta) -> None:
        message = Message(
            body=await self.serialize_message_body(meta),
            headers=self._get_headers(),
            content_type=self.definition.content_type,
            content_encoding='utf-8',
            delivery_mode=DeliveryMode.PERSISTENT if self.config.is_persistent else DeliveryMode.NOT_PERSISTENT,
        )

        answ_topic = f'{answer_params.topic}.{answer_params.id}'
        assert self._answer_exchange is not None
        await self._answer_exchange.publish(message, routing_key=answ_topic)
        self.echo.log_debug(f'sent message to {self.config.answer_exchange_name}->{answ_topic}')
