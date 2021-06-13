from unipipeline.modules.uni_module_definition import UniModuleDefinition


def parse_type(type_def: str) -> UniModuleDefinition:
    assert isinstance(type_def, str), f"type_def must be str. {type(type_def)} given"
    spec = type_def.split(":")
    assert len(spec) == 2, f'must have 2 segments. {len(spec)} was given from "{type_def}"'
    return UniModuleDefinition(
        module=spec[0],
        class_name=spec[1],
    )
