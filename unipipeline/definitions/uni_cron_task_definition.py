from uuid import UUID

from unipipeline.definitions.uni_definition import UniDefinition
from unipipeline.definitions.uni_worker_definition import UniWorkerDefinition


class UniCronTaskDefinition(UniDefinition):
    id: UUID
    name: str
    worker: UniWorkerDefinition
    when: str
    alone: bool
