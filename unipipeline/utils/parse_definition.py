from typing import Dict, Any, Set, Iterator, Tuple


def parse_definition(definitions: Dict[str, Any], defaults: Dict[str, Any], sys_names: Set[str] = None) -> Iterator[Tuple[str, Dict[str, Any]]]:
    if sys_names is None:
        sys_names = set()

    assert isinstance(definitions, dict)
    assert isinstance(sys_names, set)

    defaults = dict(defaults)
    defaults.update(definitions.get("__default__", dict()))

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
        definition = dict(defaults)
        definition.update(raw_definition)

        definition["name"] = name

        yield name, definition
