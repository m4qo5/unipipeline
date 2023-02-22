from unipipeline.definitions.uni_definition import UniDefinition
from unipipeline.definitions.uni_module_definition_message import UniModuleDefinitionMessage


class UniMessageDefinition(UniDefinition):
    name: str
    import_template: UniModuleDefinitionMessage
