from typing import NamedTuple

from unipipeline.modules.uni_module_definition import UniModuleDefinition


class UniMessageDefinition(NamedTuple):
    name: str
    type: UniModuleDefinition
