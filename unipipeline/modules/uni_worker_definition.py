from typing import Set, Any, Optional
from uuid import UUID

from unipipeline.modules.uni_broker_definition import UniBrokerDefinition
from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_message_definition import UniMessageDefinition
from unipipeline.modules.uni_module_definition import UniModuleDefinition
from unipipeline.modules.uni_waiting_definition import UniWaitingDefinition


class UniWorkerDefinition(UniDefinition):
    id: UUID
    name: str
    broker: UniBrokerDefinition[Any]
    type: Optional[UniModuleDefinition]
    topic: str
    error_topic: str
    error_payload_topic: str
    input_message: UniMessageDefinition
    output_workers: Set[str]
    ack_after_success: bool
    waitings: Set[UniWaitingDefinition]
    external: Optional[str]

    @property
    def marked_as_external(self) -> bool:
        return self.external is not None
