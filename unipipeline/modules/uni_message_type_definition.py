from typing import NamedTuple

from unipipeline.modules.uni_module_definition import UniModuleDefinition


class UniMessageTypeDefinition(NamedTuple):
    name: str
    type: UniModuleDefinition
    version: int
