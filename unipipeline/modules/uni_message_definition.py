from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_module_definition import UniModuleDefinition


class UniMessageDefinition(UniDefinition):
    name: str
    type: UniModuleDefinition
