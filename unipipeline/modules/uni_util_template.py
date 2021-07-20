from typing import Callable, Any

from jinja2 import Environment, BaseLoader


class UniUtilTemplate:

    def __init__(self) -> None:
        self._jinja2_env = Environment(loader=BaseLoader())

    def set_filter(self, name: str, filter_fn: Callable[..., str]) -> None:
        self._jinja2_env.filters[name] = filter_fn

    def template(self, definition: str, **kwargs: Any) -> str:
        if not isinstance(definition, str):
            raise TypeError(f"definition must be str. {type(definition)} given")

        template_ = self._jinja2_env.from_string(definition)
        return template_.render(**kwargs)
