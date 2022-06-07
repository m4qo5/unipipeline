from uuid import UUID

from pydantic import BaseModel, Extra


class UniAnswerParams(BaseModel):
    topic: str
    id: UUID

    class Config:
        frozen = True
        extra = Extra.ignore
