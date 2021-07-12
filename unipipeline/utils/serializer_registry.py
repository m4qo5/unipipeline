import bz2
import json
import lzma
import zlib
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Callable, Set, TypeVar, Generic
from uuid import UUID

CONTENT_TYPE__APPLICATION_JSON = 'application/json'

COMPRESSION__GZIP = 'application/x-gzip'
COMPRESSION__BZ2 = 'application/x-bz2'
COMPRESSION__LZMA = 'application/x-lzma'


T = TypeVar('T')


def prepare_content_type(content_type: str) -> str:
    if not isinstance(content_type, str):
        raise TypeError(f'content type has invalid type. must be str, {type(content_type).__name__} was given')
    return content_type.lower()


TSerializedData = TypeVar("TSerializedData")
TParsedData = TypeVar("TParsedData")


class SerializersRegistry(Generic[TParsedData, TSerializedData]):
    def __init__(self) -> None:
        self._encoders: Dict[str, Callable[[TParsedData], TSerializedData]] = dict()
        self._decoders: Dict[str, Callable[[TSerializedData], TParsedData]] = dict()
        self._content_types: Set[str] = set()

    def register(self, content_type: str, encoder: Callable[[TParsedData], TSerializedData], decoder: Callable[[TSerializedData], TParsedData]) -> None:
        content_type = prepare_content_type(content_type)
        self._encoders[content_type] = encoder
        self._decoders[content_type] = decoder
        self._content_types.add(content_type)

    def supports(self, content_type: str) -> bool:
        content_type = prepare_content_type(content_type)
        return content_type in self._content_types

    def assert_supports(self, content_type: str) -> None:
        content_type = prepare_content_type(content_type)
        if content_type not in self._content_types:
            raise TypeError(f'content_type "{content_type}" is not supported')

    def dumps(self, data: TParsedData, content_type: str) -> TSerializedData:
        content_type = prepare_content_type(content_type)
        encoder = self._encoders[content_type]
        return encoder(data)

    def loads(self, data: TSerializedData, content_type: str) -> TParsedData:
        content_type = prepare_content_type(content_type)
        decoder = self._decoders[content_type]
        return decoder(data)


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, bytes):
            return obj.hex()
        if isinstance(obj, Enum):
            return obj.value
        return json.JSONEncoder.default(self, obj)


def json_dumps(data: Any) -> str:
    return ComplexEncoder().encode(data)


serializer_registry = SerializersRegistry[Any, str]()
serializer_registry.register(CONTENT_TYPE__APPLICATION_JSON, json_dumps, json.loads)

compressor_registry = SerializersRegistry[bytes, bytes]()
compressor_registry.register(COMPRESSION__GZIP, zlib.compress, zlib.decompress)
compressor_registry.register(COMPRESSION__BZ2, bz2.compress, bz2.decompress)
compressor_registry.register(COMPRESSION__LZMA, lzma.compress, lzma.decompress)
