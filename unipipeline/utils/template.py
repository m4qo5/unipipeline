from typing import Dict, Any
from jinja2 import Environment, BaseLoader

from unipipeline.utils.camel_case import camel_case


def template(definition: str, data: Dict[str, Any]) -> str:
    assert isinstance(definition, str), f"definition must be str. {type(definition)} given"

    env = Environment(loader=BaseLoader())
    env.filters['camel'] = camel_case
    rtemplate = env.from_string(definition)
    result = rtemplate.render(**data)

    return result
