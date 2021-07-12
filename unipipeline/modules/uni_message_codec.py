from typing import Optional, Any, Union

from pydantic import BaseModel, validator


class UniMessageCodec(BaseModel):
    compression: Optional[str]
    content_type: str

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
