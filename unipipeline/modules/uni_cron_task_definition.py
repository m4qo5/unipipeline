from typing import NamedTuple
from uuid import UUID

from unipipeline.modules.uni_worker_definition import UniWorkerDefinition


class UniCronTaskDefinition(NamedTuple):
    id: UUID
    name: str
    worker: UniWorkerDefinition
    when: str
    alone: bool
