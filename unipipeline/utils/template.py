from typing import Any

from jinja2 import Environment, BaseLoader

from unipipeline.utils.camel_case import camel_case


def template(definition: str, **kwargs: Any) -> str:
    assert isinstance(definition, str), f"definition must be str. {type(definition)} given"

    env = Environment(loader=BaseLoader())
    env.filters['camel'] = camel_case
    template_ = env.from_string(definition)
    result = template_.render(**kwargs)

    return result
