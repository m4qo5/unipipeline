import asyncio
from typing import Set, List, TYPE_CHECKING, Generic, TypeVar, Type, Optional, Dict, Any

from pydantic import ValidationError

from unipipeline.brokers.uni_broker_consumer import UniBrokerConsumer
from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.definitions.uni_definition import UniDynamicDefinition
from unipipeline.utils.uni_echo import UniEcho
from unipipeline.message_meta.uni_message_meta import UniMessageMeta, UniAnswerParams

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator


TConf = TypeVar('TConf')


class UniBroker(Generic[TConf]):
    config_type: Type[TConf] = UniDynamicDefinition  # type: ignore

    def __init__(self, mediator: 'UniMediator', definition: UniBrokerDefinition, loop: asyncio.AbstractEventLoop) -> None:
        self._uni_loop = loop
        self._uni_definition = definition
        self._uni_mediator = mediator
        self._uni_echo = self._uni_mediator.echo.mk_child(f'broker[{self._uni_definition.name}]')

        try:
            self._uni_conf = self._uni_definition.configure_dynamic(self.config_type)  # type: ignore
        except ValidationError as e:
            self._uni_echo.exit_with_error(str(e))

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._uni_loop

    @property
    def config(self) -> TConf:
        return self._uni_conf

    @property
    def definition(self) -> UniBrokerDefinition:
        return self._uni_definition

    @property
    def echo(self) -> UniEcho:
        return self._uni_echo

    async def serialize_message_body(self, meta: UniMessageMeta) -> bytes:
        data_dict: Dict[str, Any] = meta.payload if meta.unwrapped else meta.dict()
        data_bytes = self._uni_mediator.serialize_content_type(self.definition.content_type, data_dict)
        data_bytes = self._uni_mediator.compress_message_body(self.definition.compression, data_bytes)
        return data_bytes

    async def parse_message_body(self, content: bytes, compression: Optional[str], content_type: str, unwrapped: bool) -> UniMessageMeta:
        content = self._uni_mediator.decompress_message_body(compression, content)
        fields = self._uni_mediator.parse_content_type(content_type, content)
        if unwrapped:
            return UniMessageMeta.create_new(fields, unwrapped=True)
        return UniMessageMeta(**fields)

    async def connect(self) -> None:
        raise NotImplementedError(f'method connect must be implemented for {type(self).__name__}')

    async def close(self) -> None:
        raise NotImplementedError(f'method close must be implemented for {type(self).__name__}')

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        raise NotImplementedError(f'method consume must be implemented for {type(self).__name__}')

    async def stop_consuming(self) -> None:
        raise NotImplementedError(f'method stop_consuming must be implemented for {type(self).__name__}')

    async def start_consuming(self) -> None:
        raise NotImplementedError(f'method start_consuming must be implemented for {type(self).__name__}')

    async def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        raise NotImplementedError(f'method publish must be implemented for {type(self).__name__}')

    async def get_topic_approximate_messages_count(self, topic: str) -> int:
        raise NotImplementedError(f'method get_topic_size must be implemented for {type(self).__name__}')

    async def initialize(self, topics: Set[str], answer_topics: Set[str]) -> None:
        raise NotImplementedError(f'method initialize must be implemented for {type(self).__name__}')

    async def get_answer(self, answer_params: UniAnswerParams, max_delay_s: int, unwrapped: bool) -> UniMessageMeta:
        raise NotImplementedError(f'method initialize must be implemented for {type(self).__name__}')

    async def publish_answer(self, answer_params: UniAnswerParams, meta: UniMessageMeta) -> None:
        raise NotImplementedError(f'method publish_answer must be implemented for {type(self).__name__}')
