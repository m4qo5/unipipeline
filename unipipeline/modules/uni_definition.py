from typing import Any, Dict, TypeVar, Type

from pydantic import BaseModel


class UniDynamicDefinition(BaseModel):
    pass


T = TypeVar('T', bound=UniDynamicDefinition)


class UniDefinition(BaseModel):
    name: str

    dynamic_props_: Dict[str, Any]

    def configure_dynamic(self, dynamic_type: Type[T]) -> T:
        if not issubclass(dynamic_type, UniDynamicDefinition):
            raise TypeError(f'dynamic prop has invalid type "{dynamic_type.__name__}". must be subclass UniDynamicDefinition')
        return dynamic_type(**self.dynamic_props_)
