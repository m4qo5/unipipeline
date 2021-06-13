from typing import List, Optional, Dict, Set, Any

from pydantic import BaseModel, validator

from unipipeline.modules.uni_broker_definition import UniBrokerDefinition
from unipipeline.modules.uni_message_type_definition import UniMessageTypeDefinition
from unipipeline.modules.uni_module_definition import UniModuleDefinition
from unipipeline.modules.uni_waiting_definition import UniWaitingDefinition

WORKER_ERROR_NAME = '__error__'


class UniWorkerDefinition(BaseModel):
    name: str
    broker: UniBrokerDefinition[Any]
    type: UniModuleDefinition
    topic: str
    prefetch: int
    input_message: UniMessageTypeDefinition
    output_workers: List[str]
    retry_max_count: int
    retry_delay_s: int
    max_ttl_s: Optional[int]
    auto_ack: bool
    waitings: List[UniWaitingDefinition]

    @validator("prefetch")
    def prefetch_must_be_gte_1(cls, v: int) -> Optional[int]:
        if v < 1:
            raise ValueError("must be >= 1")
        return v

    @validator("retry_max_count")
    def retry_max_count_must_be_gt_0(cls, v: int) -> Optional[int]:
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    @validator("retry_delay_s")
    def retry_delay_s_must_be_gt_0(cls, v: int) -> Optional[int]:
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    def get_related_broker_names(self, index_of_workers: Dict[str, 'UniWorkerDefinition']) -> Set[str]:
        result = {self.broker.name, index_of_workers[WORKER_ERROR_NAME].broker.name}
        for w_name in self.output_workers:
            result.add(index_of_workers[w_name].broker.name)
        return result

    def get_related_worker_names(self) -> Set[str]:
        result = {self.name, WORKER_ERROR_NAME}
        for w_name in self.output_workers:
            result.add(w_name)
        return result

    def wait_everything(self) -> None:
        for wd in self.waitings:
            wd.wait()
