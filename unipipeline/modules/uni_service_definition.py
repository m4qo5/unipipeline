from typing import NamedTuple
from uuid import UUID


class UniServiceDefinition(NamedTuple):
    id: UUID
    name: str
