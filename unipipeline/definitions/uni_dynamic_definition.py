from pydantic import BaseModel, Extra


class UniDynamicDefinition(BaseModel):
    class Config:
        extra = Extra.ignore
        allow_mutation = False
        frozen = True
