from typing import Dict, Any, Set, Iterator, Tuple
from uuid import uuid4


COMMON_PROP = "__default__"


class ParseDefinitionError(Exception):
    pass


def parse_definition(
    conf_name: str,
    definitions: Dict[str, Any],
    defaults: Dict[str, Any],
    required_keys: Set[str]
) -> Iterator[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    if not isinstance(definitions, dict):
        raise ParseDefinitionError(f'definition of {conf_name} has invalid type. must be dict')

    common = definitions.get(COMMON_PROP, dict())

    for name, raw_definition in definitions.items():
        other_props: Dict[str, Any] = dict()

        name = str(name)
        if name.startswith("_"):
            if name == COMMON_PROP:
                continue
            else:
                raise ValueError(f"key '{name}' is not acceptable")

        if not isinstance(raw_definition, dict):
            raise ParseDefinitionError(f'definition of {conf_name}->{name} has invalid type. must be dict')

        result_definition = dict(defaults)
        combined_definition = dict(common)
        combined_definition.update(raw_definition)

        for k, v in combined_definition.items():
            if k in defaults:
                result_definition[k] = v
                vd = defaults.get(k, None)
                if vd is not None and type(vd) != type(v):
                    raise ParseDefinitionError(f'definition of {conf_name}->{name} has invalid key "{k}" type')
            elif k in required_keys:
                result_definition[k] = v
            else:
                other_props[k] = v

        for rk in required_keys:
            if rk not in result_definition:
                raise ParseDefinitionError(f'definition of {conf_name}->{name} has no required prop "{rk}"')

        result_definition["name"] = name
        result_definition["id"] = uuid4()

        yield name, result_definition, other_props
