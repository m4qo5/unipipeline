from uuid import UUID

from pydantic import BaseModel, Extra


class UniAnswerParams(BaseModel):
    topic: str
    id: UUID
    ttl_s: int

    class Config:
        frozen = True
        extra = Extra.ignore
