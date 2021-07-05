from typing import Dict, Any
from uuid import UUID

from unipipeline.modules.uni_definition import UniDefinition


class UniExternalDefinition(UniDefinition):
    id: UUID
    name: str

    _dynamic_props_: Dict[str, Any]
