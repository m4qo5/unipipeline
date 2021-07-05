from uuid import UUID

from unipipeline.modules.uni_definition import UniDefinition


class UniExternalDefinition(UniDefinition):
    id: UUID
    name: str
