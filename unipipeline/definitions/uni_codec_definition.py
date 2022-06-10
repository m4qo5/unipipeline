from unipipeline.definitions.uni_definition import UniDefinition
from unipipeline.definitions.uni_module_definition import UniModuleDefinition


class UniCodecDefinition(UniDefinition):
    name: str
    encoder_type: UniModuleDefinition
    decoder_type: UniModuleDefinition
