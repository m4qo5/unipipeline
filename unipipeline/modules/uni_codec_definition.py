from unipipeline import UniModuleDefinition, UniDefinition


class UniCodecDefinition(UniDefinition):
    name: str
    encoder_type: UniModuleDefinition
    decoder_type: UniModuleDefinition
