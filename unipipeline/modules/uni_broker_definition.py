from typing import Generic, TypeVar
from uuid import UUID

from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_message_codec import UniMessageCodec
from unipipeline.modules.uni_module_definition import UniModuleDefinition

TContent = TypeVar('TContent')


class UniBrokerDefinition(UniDefinition, Generic[TContent]):
    id: UUID
    type: UniModuleDefinition

    retry_max_count: int
    retry_delay_s: int

    codec: UniMessageCodec[TContent]
