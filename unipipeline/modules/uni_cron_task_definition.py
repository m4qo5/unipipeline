from typing import NamedTuple

from unipipeline.modules.uni_worker_definition import UniWorkerDefinition


class UniCronTaskDefinition(NamedTuple):
    name: str
    worker: UniWorkerDefinition
    when: str
