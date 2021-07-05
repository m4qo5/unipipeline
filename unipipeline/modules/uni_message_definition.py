from typing import Dict, Any

from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_module_definition import UniModuleDefinition


class UniMessageDefinition(UniDefinition):
    name: str
    type: UniModuleDefinition

    _dynamic_props_: Dict[str, Any]
