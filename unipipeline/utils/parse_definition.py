from typing import Dict, Any, Set, Iterator


def parse_definition(definitions: Dict[str, Any], sys_names: Set[str] = None) -> Iterator[Dict[str, Any]]:
    if sys_names is None:
        sys_names = set()

    assert isinstance(definitions, dict)
    assert isinstance(sys_names, set)

    default = definitions["__default__"]
    assert isinstance(definitions["__default__"], dict), f"definitions['__default__'] must be dict. {type(definitions['__default__'])} given"
    for name, raw_definition in definitions.items():
        name = str(name)
        if name.startswith("_"):
            if name == "__default__":
                continue
            elif name in sys_names:
                pass
            else:
                raise ValueError(f"key '{name}' is not acceptable")
        assert isinstance(raw_definition, dict)
        definition = dict(default)
        definition.update(raw_definition)

        definition["name"] = name

        yield definition
