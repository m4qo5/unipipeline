import importlib
from functools import cached_property
from time import sleep
from types import ModuleType
from typing import TypeVar, Any, Optional, Callable, Generic, Union

T = TypeVar('T')


class UniModuleDefinition(Generic[T]):
    ASSIGNED_TYPE_IS_INSTANCE: bool = False
    ASSIGNED_TYPE: Optional[T] = None  # if None => Function
    CREATE_TEMPLATE: str = ''

    @classmethod
    def validate_value(cls, value: Any) -> Optional[str]:
        if value is None:
            return 'value could not be None'

        if cls.ASSIGNED_TYPE is None:
            return None if callable(value) else f'object is not callable. {type(value).__name__} was given'

        if cls.ASSIGNED_TYPE_IS_INSTANCE:
            return None if isinstance(value, cls.ASSIGNED_TYPE) else f'value has not valid type. must be {cls.ASSIGNED_TYPE.__name__}. {type(value).__name__} was given'

        try:
            return None if issubclass(value, cls.ASSIGNED_TYPE) else f'value is not subclass of {cls.ASSIGNED_TYPE.__name__}'
        except Exception:  # noqa
            return f'value is not subclass of {cls.ASSIGNED_TYPE.__name__}'

    def __init__(self, import_template: Union[str, T]) -> None:
        self._import_value: Optional[T] = None
        if not isinstance(import_template, str):
            if err := self.validate_value(import_template):
                raise ValueError(err)
            self._import_value = import_template
            self._import_template = ''
        else:
            self._import_template = import_template

        self._module = ''
        self._object_name = ''

    def patch_import_template(self, transform: Optional[Callable[[str], Union[str, T]]] = None) -> None:
        if self._import_value is not None:
            return
        tpl: str = self._import_template
        if transform is not None:
            transformation_res = transform(tpl)
            if not isinstance(transformation_res, str):
                self._import_value = transformation_res
                if name := getattr(transformation_res, '__name__', None):
                    self._object_name = name
                return
            tpl = transformation_res
        spec = tpl.split(":")
        if len(spec) != 2:
            raise ValueError(f'must have 2 segments. {len(spec)} was given from "{self._import_template}"')
        self._module = spec[0]
        self._object_name = spec[1]
        self._import_value = None

    def _load_mdl(self, cache: bool = True, critical: bool = False, retries: int = 0, retry_delay: float = 1.) -> Optional[ModuleType]:
        for i in range(retries + 1):  # because fs has cache time
            if not cache:
                importlib.invalidate_caches()
            try:
                return importlib.import_module(self.module)
            except ModuleNotFoundError:
                if retries > 0 and retries == i:
                    if critical:
                        raise
                    return None
                else:
                    sleep(retry_delay)  # cache of FS
        return None

    @cached_property
    def import_value(self) -> T:
        if self._import_value is not None:
            return self._import_value
        mdl = self._load_mdl(critical=True)
        assert mdl is not None
        value = getattr(mdl, self.object_name, None)
        if err := self.validate_value(value):
            raise ValueError(err)
        self._import_value = value
        return value  # type: ignore

    @property
    def module(self) -> str:
        return self._module

    @property
    def object_name(self) -> str:
        return self._object_name


def test_uni_module_definition() -> None:
    from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
    i = UniModuleDefinition('unipipeline.definitions.%1:UniBrokerDefinition')  # type: ignore
    i.ASSIGNED_TYPE = UniBrokerDefinition
    i.patch_import_template(lambda x: x.replace('%1', 'uni_broker_definition'))
    assert i.import_value == UniBrokerDefinition

    i = UniModuleDefinition(UniBrokerDefinition)
    i.ASSIGNED_TYPE = UniBrokerDefinition
    i.patch_import_template(lambda x: x.replace('%1', 'uni_broker_definition'))
    assert i.import_value == UniBrokerDefinition
