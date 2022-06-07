from pydantic import BaseModel, Extra


class UniMessage(BaseModel):
    class Config:
        extra = Extra.ignore
