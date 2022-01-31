from typing import Dict, Any, Optional

import yaml
from pydantic import ValidationError

from unipipeline.config.uni_config_definition import UniConfigDefinition


class UniConfigLoader:
    def load(self) -> UniConfigDefinition:
        raise NotImplementedError(f'method "load" must be specified for class "{type(self).__name__}"')


class UniDictConfigLoader(UniConfigLoader):
    __slots__ = (
        '_value',
        '_cache',
    )

    def __init__(self, value: Dict[str, Any]) -> None:
        self._value = value
        self._cache: Optional[UniConfigDefinition] = None

    def load(self) -> UniConfigDefinition:
        if self._cache is not None:
            return self._cache
        self._cache = UniConfigDefinition(**self._value)
        return self._cache


class UniYamlFileConfigLoader(UniConfigLoader):
    __slots__ = (
        '_file_path',
        '_file_cache',
    )

    def __init__(self, file_path: str) -> None:
        if not isinstance(file_path, str):
            TypeError(f'file_path must be string. {type(file_path).__name__} was given')
        self._file_path = file_path
        self._file_cache: Optional[UniConfigDefinition] = None

    def load(self) -> UniConfigDefinition:
        if self._file_cache is not None:
            return self._file_cache
        with open(self._file_path, "rt") as f:
            content = dict(yaml.safe_load(f))
            self._file_cache = UniConfigDefinition(**content)
        return self._file_cache
