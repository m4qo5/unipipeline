import logging
import uuid
from typing import Union

from pydantic import Field, UUID4, BaseModel


class UniServiceDefinition(BaseModel):
    name: str
    id: UUID4 = Field(default_factory=lambda: uuid.uuid4())
    echo_colors: bool = True
    echo_level: Union[int, str] = logging.INFO

    @property
    def colors(self) -> bool:  # TODO: remove
        return self.echo_colors
