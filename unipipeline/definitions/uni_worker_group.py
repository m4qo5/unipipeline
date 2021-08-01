from typing import List

from unipipeline.definitions.uni_definition import UniDefinition


class UniWorkerGroupDefinition(UniDefinition):
    worker_names: List[str]
