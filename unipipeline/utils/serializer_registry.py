import bz2
import json
import lzma
import zlib
from typing import Dict, Any, Callable, Set, Type, TypeVar, Generic

CONTENT_TYPE__APPLICATION_JSON = 'application/json'
CONTENT_TYPE__APPLICATION_DATA = 'application/data'

COMPRESSION__GZIP = 'application/x-gzip'
COMPRESSION__BZ2 = 'application/x-bz2'
COMPRESSION__LZMA = 'application/x-lzma'

CONTENT_TYPE__BINARY = 'binary'
CONTENT_ENCODING__UTF8 = 'utf-8'


TContentEncoding = str
TContentType = str


T = TypeVar('T')


def ensure_type(name: str, value: T, constructor: Type[T]) -> T:
    if not isinstance(value, constructor):
        raise TypeError(f'{name} has invalid type. must be {constructor.__name__}, {type(value).__name__} was given')
    return value


def prepare_content_type(content_type: TContentType) -> TContentType:
    ensure_type('content_type', content_type, TContentType)
    return content_type.lower()


TSerializedData = TypeVar("TSerializedData")
TParsedData = TypeVar("TParsedData")


class SerializersRegistry(Generic[TParsedData, TSerializedData]):
    def __init__(self) -> None:
        self._encoders: Dict[TContentType, Callable[[TParsedData], TSerializedData]] = dict()
        self._decoders: Dict[TContentType, Callable[[TSerializedData], TParsedData]] = dict()
        self._content_types: Set[TContentType] = set()

    def register(self, content_type: TContentType, encoder: Callable[[TParsedData], TSerializedData], decoder: Callable[[TSerializedData], TParsedData]) -> None:
        content_type = prepare_content_type(content_type)
        self._encoders[content_type] = encoder
        self._decoders[content_type] = decoder
        self._content_types.add(content_type)

    def supports(self, content_type: TContentType) -> bool:
        content_type = prepare_content_type(content_type)
        return content_type in self._content_types

    def assert_supports(self, content_type: TContentType) -> None:
        content_type = prepare_content_type(content_type)
        if content_type not in self._content_types:
            raise TypeError(f'content_type "{content_type}" is not supported')

    def dumps(self, data: TParsedData, content_type: TContentType) -> TSerializedData:
        content_type = prepare_content_type(content_type)
        encoder = self._encoders[content_type]
        return encoder(data)

    def loads(self, data: TSerializedData, content_type: TContentType) -> TParsedData:
        content_type = prepare_content_type(content_type)
        decoder = self._decoders[content_type]
        return decoder(data)


serializer_registry = SerializersRegistry[Any, str]()
serializer_registry.register(CONTENT_TYPE__APPLICATION_JSON, json.dumps, json.loads)

compressor_registry = SerializersRegistry[bytes, bytes]()
compressor_registry.register(COMPRESSION__GZIP, zlib.compress, zlib.decompress)
compressor_registry.register(COMPRESSION__BZ2, bz2.compress, bz2.decompress)
compressor_registry.register(COMPRESSION__LZMA, lzma.compress, lzma.decompress)
