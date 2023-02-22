from unipipeline.definitions.uni_definition import UniDefinition
from unipipeline.definitions.uni_module_definition_fn import UniModuleDefinitionFn


class UniCodecDefinition(UniDefinition):
    name: str
    encoder_type: UniModuleDefinitionFn
    decoder_type: UniModuleDefinitionFn
