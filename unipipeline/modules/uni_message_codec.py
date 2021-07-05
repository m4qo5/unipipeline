from typing import Generic, Optional, Dict, Any, TypeVar

from pydantic import BaseModel, validator

from unipipeline.utils.serializer_registry import serializer_registry, compressor_registry


TContent = TypeVar('TContent')


class UniMessageCodec(BaseModel, Generic[TContent]):
    compression: Optional[str]
    content_type: str

    @validator("content_type")
    def content_type_must_be_supported(cls, v: str) -> str:
        serializer_registry.assert_supports(v)
        return v

    @validator("compression")
    def compression_must_be_supported(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        compressor_registry.assert_supports(v)
        return v

    def decompress(self, data: bytes) -> bytes:
        if self.compression is not None:
            return compressor_registry.loads(data, self.compression)
        return data

    def loads(self, data: TContent) -> Dict[str, Any]:
        return serializer_registry.loads(data, self.content_type)  # type: ignore

    def compress(self, data: TContent) -> TContent:
        if self.compression is not None:
            data_bytes: bytes
            if isinstance(data, str):
                data_bytes = bytes(data, encoding='utf-8')
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                raise TypeError('invalid type')
            return compressor_registry.dumps(data_bytes, self.compression).decode("utf-8")  # type: ignore
        return data

    def dumps(self, data: Dict[str, Any]) -> str:
        return serializer_registry.dumps(data, self.content_type)
