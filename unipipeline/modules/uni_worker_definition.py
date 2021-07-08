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
    broker: UniBrokerDefinition
    type: Optional[UniModuleDefinition]
    topic: str
    error_topic: str
    error_payload_topic: str
    answer_topic: str
    input_message: UniMessageDefinition
    output_message: Optional[UniMessageDefinition]
    output_workers: Set[str]
    ack_after_success: bool
    waitings: Set[UniWaitingDefinition]
    external: Optional[str]

    @property
    def marked_as_external(self) -> bool:
        return self.external is not None

    @property
    def need_answer(self) -> bool:
        return self.output_message is not None
