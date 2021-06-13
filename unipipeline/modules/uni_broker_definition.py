from typing import Optional, Any, Dict, Tuple, TypeVar, Generic

from pydantic import BaseModel, validator

from unipipeline.modules.uni_module_definition import UniModuleDefinition
from unipipeline.utils.serializer_registry import serializer_registry, compressor_registry

SUPPORTED_ENCODINGS = {'utf-8', }


TContent = TypeVar('TContent')


class UniMessageCodec(BaseModel, Generic[TContent]):
    compression: Optional[str]
    content_type: str

    @validator("content_type")
    def content_type_must_be_supported(cls, v: str) -> str:
        serializer_registry.assert_supports(v)
        return v

    @validator("compression")
    def compression_must_be_supported(cls, v: Optional[str]) -> Optional[str]:
        compressor_registry.assert_supports(v)
        return v

    def decompress(self, data: TContent) -> TContent:
        if self.compression is not None:
            return compressor_registry.loads(data, self.compression)
        return data

    def loads(self, data: TContent) -> Dict[str, Any]:
        return serializer_registry.loads(data, self.content_type)

    def compress(self, data: TContent) -> TContent:
        if self.compression is not None:
            return compressor_registry.dumps(data, self.compression)
        return data

    def dumps(self, data: Dict[str, Any]) -> TContent:
        return serializer_registry.dumps(data, self.content_type)


class UniBrokerKafkaPropsDefinition(BaseModel):
    bootstrap_servers: str
    api_version: Tuple[int, int]


class UniBrokerRMQPropsDefinition(BaseModel):
    exchange_name: str
    heartbeat: int
    blocked_connection_timeout: int
    socket_timeout: int
    stack_timeout: int
    exchange_type: str


class UniBrokerDefinition(BaseModel, Generic[TContent]):
    name: str
    type: UniModuleDefinition
    retry_max_count: int
    retry_delay_s: int
    passive: bool
    durable: bool
    auto_delete: bool
    is_persistent: bool
    message_codec: UniMessageCodec[TContent]
    kafka_definition: UniBrokerKafkaPropsDefinition
    rmq_definition: UniBrokerRMQPropsDefinition
