from importlib import import_module
from typing import NamedTuple, Generic, Type, TypeVar

T = TypeVar('T')


class UniModuleDefinition(NamedTuple, Generic[T]):
    module: str
    class_name: str

    def import_class(self, type: Type[T]) -> Type[T]:
        mdl = import_module(self.module)
        tp = getattr(mdl, self.class_name)
        assert issubclass(tp, type)
        return tp
