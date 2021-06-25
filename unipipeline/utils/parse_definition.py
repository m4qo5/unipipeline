from functools import reduce
from operator import or_
from typing import Dict, Any, Set, Iterator, Tuple


class ParseDefinitionError(Exception):
    pass


def parse_definition(conf_name: str, definitions: Dict[str, Any], defaults: Dict[str, Any], required: Set[str], sys_names: Set[str] = None) -> Iterator[Tuple[str, Dict[str, Any]]]:
    if sys_names is None:
        sys_names = set()

    if not isinstance(definitions, dict):
        raise ParseDefinitionError(f'definition of {conf_name} has invalid type. must be dict')

    max_set_of_keys = reduce(or_, [set(defaults.keys()), required])
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

        if not isinstance(raw_definition, dict):
            raise ParseDefinitionError(f'definition of {conf_name}->{name} has invalid type. must be dict')

        definition = dict(defaults)
        definition.update(raw_definition)

        definition_keys = set(definition.keys())

        for k, v in definition.items():
            vd = defaults.get(k, None)
            if vd is not None and type(vd) != type(v):
                raise ParseDefinitionError(f'definition of {conf_name}->{name} has invalid key "{k}" type')

        if max_set_of_keys != definition_keys:
            required_props = max_set_of_keys.difference(definition_keys)
            invalid_props = definition_keys.difference(max_set_of_keys)
            if len(invalid_props) > 0:
                raise ParseDefinitionError(f'definition of {conf_name}->{name} has invalid extra props: {", ".join(invalid_props)}')
            if len(required_props) > 0:
                raise ParseDefinitionError(f'definition of {conf_name}->{name} has no required props: {", ".join(required_props)}')
            raise ParseDefinitionError(f'definition of {conf_name}->{name} has invalid props: {max_set_of_keys}!={definition_keys}')

        definition["name"] = name

        yield name, definition
