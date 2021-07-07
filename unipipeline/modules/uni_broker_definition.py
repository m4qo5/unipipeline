from typing import TypeVar, Optional
from uuid import UUID

from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_message_codec import UniMessageCodec
from unipipeline.modules.uni_module_definition import UniModuleDefinition

TContent = TypeVar('TContent')


class UniBrokerDefinition(UniDefinition):
    id: UUID
    type: UniModuleDefinition

    retry_max_count: int
    retry_delay_s: int

    external: Optional[str]

    codec: UniMessageCodec

    @property
    def marked_as_external(self) -> bool:
        return self.external is not None
