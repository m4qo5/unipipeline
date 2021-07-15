from unipipeline.modules.uni_module_definition import UniModuleDefinition
from unipipeline.modules.uni_definition import UniDefinition


class UniCodecDefinition(UniDefinition):
    name: str
    encoder_type: UniModuleDefinition
    decoder_type: UniModuleDefinition
