from typing import Optional, Any, Union

from pydantic import BaseModel, validator

from unipipeline.utils.serializer_registry import serializer_registry, compressor_registry


class UniMessageCodec(BaseModel):
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

    def decompress(self, data: Union[bytes, str]) -> bytes:
        data_bytes: bytes
        if isinstance(data, str):
            data_bytes = bytes(data, encoding='utf-8')
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            raise TypeError('invalid type')
        if self.compression is not None:
            return compressor_registry.loads(data_bytes, self.compression)  # type: ignore
        return data_bytes

    def loads(self, data: Union[bytes, str]) -> Any:
        data_str: str
        if isinstance(data, str):
            data_str = data
        elif isinstance(data, bytes):
            data_str = data.decode('utf-8')
        else:
            raise TypeError('invalid type')

        return serializer_registry.loads(data_str, self.content_type)  # type: ignore

    def dumps(self, data: Any) -> str:
        return serializer_registry.dumps(data, self.content_type)

    def compress(self, data: Union[bytes, str]) -> bytes:
        data_bytes: bytes
        if isinstance(data, str):
            data_bytes = bytes(data, encoding='utf-8')
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            raise TypeError('invalid type')

        if self.compression is not None:
            return compressor_registry.dumps(data_bytes, self.compression)
        return data_bytes
