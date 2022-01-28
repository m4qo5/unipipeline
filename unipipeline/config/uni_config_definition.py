from enum import Enum
from typing import Tuple, Optional, Dict, Any
from uuid import UUID

from pydantic import BaseModel, Extra

APPLICATION_JSON = 'application/json'
COMPRESSION_GZIP = "application/x-gzip"
COMPRESSION_BZ2 = "application/x-bz2"
COMPRESSION_LZMA = "application/x-lzma"


class UniBrokerConfigDefinition(BaseModel):
    id: UUID
    name: str

    import_template: str
    retry_max_count: int = 3
    retry_delay_s: int = 10
    content_type: str = APPLICATION_JSON
    compression: Optional[str] = None
    external: Optional[str] = None
    _external_props_ = Dict[str, Any] = dict()

    class Config:
        extra = Extra.forbid


class UniMessageConfigDefinition(BaseModel):
    id: UUID
    name: str

    import_template: str

    class Config:
        extra = Extra.forbid


class UniWaitingConfigDefinition(BaseModel):
    id: UUID
    name: str

    import_template: str

    class Config:
        extra = Extra.forbid


class UniWorkerConfigDefinition(BaseModel):
    id: UUID
    name: str

    import_template: str
    input_message: str
    output_workers: Tuple[str, ...]
    answer_message: Optional[str]
    answer_unwrapped: Optional[bool]
    waiting_for: Tuple[str, ...]
    external: Optional[str]
    _external_props_ = Dict[str, Any] = dict()

    class Config:
        extra = Extra.forbid


class UniCronConfigDefinition(BaseModel):
    id: UUID
    name: str
    worker: str
    when: Optional[str]
    every_sec: Optional[int]

    class Config:
        extra = Extra.forbid


class UniEchoLevelConfigDefinition(Enum):
    debug = 'debug'
    info = 'info'
    warning = 'warning'
    error = 'error'


class UniServiceConfigDefinition(BaseModel):
    id: UUID
    name: str
    echo_colors: bool = True
    echo_level: UniEchoLevelConfigDefinition = UniEchoLevelConfigDefinition.warning

    class Config:
        extra = Extra.forbid


class UniExternalServiceConfigDefinition(BaseModel):
    name: str


class UniConfigDefinition(BaseModel):
    service: UniServiceConfigDefinition
    external: Dict[str, Any] = dict()
    brokers: Dict[str, UniBrokerConfigDefinition]
    messages: Dict[str, UniMessageConfigDefinition]
    cron: Dict[str, UniCronConfigDefinition]
    waitings: Dict[str, UniWaitingConfigDefinition]
    workers: Dict[str, UniWorkerConfigDefinition]
    external: Dict[str, UniExternalServiceConfigDefinition]

    class Config:
        extra = Extra.forbid
