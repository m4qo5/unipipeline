from typing import Tuple, TypeVar, Generic, Dict, Any
from uuid import UUID

from pydantic import BaseModel

from unipipeline.modules.uni_message_codec import UniMessageCodec
from unipipeline.modules.uni_module_definition import UniModuleDefinition


class UniBrokerKafkaPropsDefinition(BaseModel):
    api_version: Tuple[int, int]


class UniAmqpBrokerPropsDefinition(BaseModel):
    exchange_name: str
    heartbeat: int
    blocked_connection_timeout: int
    socket_timeout: int
    stack_timeout: int
    exchange_type: str


TContent = TypeVar('TContent')


class UniBrokerDefinition(BaseModel, Generic[TContent]):
    id: UUID
    name: str
    type: UniModuleDefinition

    # _dynamic_data: Dict[str, Any]

    retry_max_count: int
    retry_delay_s: int
    passive: bool
    durable: bool
    auto_delete: bool
    is_persistent: bool
    message_codec: UniMessageCodec[TContent]
    kafka_definition: UniBrokerKafkaPropsDefinition
    rmq_definition: UniAmqpBrokerPropsDefinition
