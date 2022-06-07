from pydantic import BaseModel, Extra

from unipipeline.message_meta.uni_message_meta_error_topic import UniMessageMetaErrTopic


class UniMessageMetaErr(BaseModel):
    error_topic: UniMessageMetaErrTopic
    error_type: str
    error_message: str
    retry_times: int

    class Config:
        extra = Extra.ignore
        frozen = True
