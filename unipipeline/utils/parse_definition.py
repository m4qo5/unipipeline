from typing import Dict, Any, Set, Iterator, Tuple
from uuid import uuid4


COMMON_PROP = "__default__"


class ParseDefinitionError(Exception):
    pass


def parse_definition(
    conf_name: str,
    definitions: Dict[str, Any],
    defaults: Dict[str, Any],
    required_keys: Set[str],
    sys_names: Set[str] = None
) -> Iterator[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    if sys_names is None:
        sys_names = set()

    if not isinstance(definitions, dict):
        raise ParseDefinitionError(f'definition of {conf_name} has invalid type. must be dict')

    defaults_keys = set(defaults.keys())

    common = definitions.get(COMMON_PROP, dict())

    for name, raw_definition in definitions.items():
        other_props: Dict[str, Any] = dict()
        definition = dict(defaults)

        name = str(name)
        if name.startswith("_"):
            if name == COMMON_PROP:
                continue
            elif name in sys_names:
                pass
            else:
                raise ValueError(f"key '{name}' is not acceptable")

        if not isinstance(raw_definition, dict):
            raise ParseDefinitionError(f'definition of {conf_name}->{name} has invalid type. must be dict')

        raw_definition.update(common)

        for k, v in raw_definition.items():
            if k in defaults_keys:
                definition[k] = v
                vd = defaults.get(k, None)
                if vd is not None and type(vd) != type(v):
                    raise ParseDefinitionError(f'definition of {conf_name}->{name} has invalid key "{k}" type')
            elif k in required_keys:
                definition[k] = v
            else:
                other_props[k] = v

        for rk in required_keys:
            if rk not in definition:
                raise ParseDefinitionError(f'definition of {conf_name}->{name} has no required prop "{rk}"')

        definition["name"] = name
        definition["id"] = uuid4()

        yield name, definition, other_props
