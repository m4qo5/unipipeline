from enum import Enum
from typing import Tuple, Optional, Dict, Any, Type, Callable
from uuid import UUID, uuid4

from pydantic import BaseModel, Extra, root_validator, validator

APPLICATION_JSON = 'application/json'
COMPRESSION_GZIP = "application/x-gzip"
COMPRESSION_BZ2 = "application/x-bz2"
COMPRESSION_LZMA = "application/x-lzma"


DEFAULT_BROKER = 'default_broker'
DEFAULT_CODEC = APPLICATION_JSON


_CONF_PROP = '_config_'


class UniBaseConfigDefinition(BaseModel):
    id: UUID
    name: str

    class Config:
        extra = Extra.forbid
        allow_mutation = False

    @root_validator(pre=True)
    def validate(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if _CONF_PROP in values:
            values.pop(_CONF_PROP)
        return values


class TemplateStr(BaseModel):
    __root__: str


class ImportLoaderTemplateStr(BaseModel):
    __root__: TemplateStr


class UniBrokerConfigDefinition(UniBaseConfigDefinition):
    import_template: ImportLoaderTemplateStr
    retry_max_count: int = 3
    retry_delay_s: int = 10
    external: Optional[str] = None
    compression: Optional[str] = None
    content_type: str = DEFAULT_CODEC
    _extra_props_: Dict[str, Any] = dict()


class UniMessageConfigDefinition(UniBaseConfigDefinition):
    import_template: ImportLoaderTemplateStr


class UniWaitingConfigDefinition(UniBaseConfigDefinition):
    import_template: ImportLoaderTemplateStr
    retry_max_count: int
    retry_delay_s: int


class UniWorkerConfigDefinition(UniBaseConfigDefinition):
    import_template: ImportLoaderTemplateStr
    input_message: str
    topic: TemplateStr = TemplateStr(__root__='{{name}}')
    error_payload_topic: TemplateStr = TemplateStr(__root__='{{topic}}__error__payload')
    answer_topic: str = TemplateStr(__root__='{{name}}__answer')
    broker: Optional[str] = None  # "default_broker" if None
    external: Optional[str] = None
    answer_message: Optional[str] = None
    answer_avg_delay_s: int = 3
    prefetch_count: int = 1
    input_unwrapped: bool = False
    ack_after_success: bool = True
    output_workers: Tuple[str, ...] = tuple()
    answer_unwrapped: bool = False
    waiting_for: Tuple[str, ...] = tuple()
    _extra_props_: Dict[str, Any] = dict()

    @root_validator()
    def validate(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        conf = values.pop(_CONF_PROP)

        br = values.get('broker', DEFAULT_BROKER)
        if br not in conf.get('brokers', dict()):
            raise ValueError(f'broker "{br}" was not defined')

        im = values.get('input_message')
        if im is not None and im not in conf.get('messages', dict()):
            raise ValueError(f'input_message "{im}" was not defined')

        om = values.get('answer_message')
        if om is not None and om not in conf.get('messages', dict()):
            raise ValueError(f'answer_message "{om}" was not defined')

        for ow in values.get('output_workers', []):
            if ow not in conf.get('workers'):
                raise ValueError(f'output_worker "{ow}" was not defined in workers list')

        ext = values.get('external', None)
        if ext is not None and ext not in conf.get('external', dict()):
            raise ValueError(f'external "{ext}" was not defined')

        for wt in values.get('waiting_for', []):
            if wt not in conf.get('waitings'):
                raise ValueError(f'waiting "{wt}" was not defined')

        return values


def check_var_name(value: str) -> str:
    if value
    return value


class UniCronConfigDefinition(UniBaseConfigDefinition):
    worker: str
    when: Optional[str]
    every_sec: Optional[int]

    @validator('worker')
    def validate_worker(cls, v, values) -> str:
        if len(v) == 0:
            raise ValueError('invalid format of worker name')
        return v

    @root_validator(pre=True)
    def validate(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        conf = values.pop(_CONF_PROP)

        worker = values.get('worker', None)
        if worker is not None and worker not in conf.get('workers', dict()):
            raise ValueError(f'worker "{worker}" was not defined')

        if not ((values.get('when') is None) ^ (values.get('every_sec') is None)):
            raise ValueError('when or every_spec must be specified and one from it must be None')

        return values


class UniEchoLevelConfigDefinition(Enum):
    debug = 'debug'
    info = 'info'
    warning = 'warning'
    error = 'error'


class UniServiceConfigDefinition(UniBaseConfigDefinition):
    echo_colors: bool = True
    echo_level: UniEchoLevelConfigDefinition = UniEchoLevelConfigDefinition.warning

    @root_validator(pre=True)
    def validate(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if 'id' not in values:
            values['id'] = uuid4()
        return values


class UniCodecConfigDefinition(UniBaseConfigDefinition):
    encoder_import_template: str
    decoder_import_template: str


class UniExternalServiceConfigDefinition(UniBaseConfigDefinition):
    pass


class UniConfigDefinition(BaseModel):
    service: UniServiceConfigDefinition
    codecs: Dict[str, UniCodecConfigDefinition]
    brokers: Dict[str, UniBrokerConfigDefinition]
    messages: Dict[str, UniMessageConfigDefinition]
    cron: Dict[str, UniCronConfigDefinition]
    waitings: Dict[str, UniWaitingConfigDefinition]
    workers: Dict[str, UniWorkerConfigDefinition]
    external: Dict[str, UniExternalServiceConfigDefinition]

    class Config:
        extra = Extra.forbid

    @root_validator(pre=True)
    def validate(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        extra_default_name(values, 'codecs', UniCodecConfigDefinition, defaults={
            COMPRESSION_GZIP: {
                'encoder_import_template': 'zlib:compress',
                'decoder_import_template': 'zlib:decompress',
            },
            COMPRESSION_BZ2: {
                'encoder_import_template': 'bz2:compress',
                'decoder_import_template': 'bz2:decompress',
            },
            COMPRESSION_LZMA: {
                'encoder_import_template': 'lzma:compress',
                'decoder_import_template': 'lzma:decompress',
            },
            APPLICATION_JSON: {
                'encoder_import_template': 'unipipeline.utils.complex_serializer:complex_serializer_json_dumps',
                'decoder_import_template': 'json:loads',
            },
        })
        extra_default_name(values, 'messages', UniMessageConfigDefinition, defaults={
            'uni_cron_message': {
                'import_template': "unipipeline.message.uni_cron_message:UniCronMessage"
            },
        })
        extra_default_name(values, 'cron', UniCronConfigDefinition)
        extra_default_name(values, 'brokers', UniBrokerConfigDefinition)
        extra_default_name(values, 'waitings', UniWaitingConfigDefinition)
        extra_default_name(values, 'external', UniExternalServiceConfigDefinition)
        extra_default_name(values, 'workers', UniWorkerConfigDefinition)
        return values


def extra_default_name(values: Dict[str, Any], prop_name: str, model: Type[BaseModel], *, set_default_dict: bool = True, defaults: Optional[Dict[str, Any]] = None) -> None:
    if not isinstance(values, dict):
        raise TypeError(f'{prop_name}. invalid type of values. must be dict. {type(values).__name__} was given')

    if prop_name not in values:
        if set_default_dict:
            values[prop_name] = defaults or dict()
        return

    if defaults is not None:
        defaults.update(values[prop_name])
        values[prop_name] = defaults

    default = dict()
    if '__default__' in values[prop_name]:
        default = values[prop_name].pop('__default__')
        if not isinstance(default, dict):
            raise TypeError(f'{prop_name}->__default__. invalid type. must be dict. {type(default).__name__} was given')

    model_files = set(model.__fields__.keys())

    for k in values[prop_name].keys():
        if not isinstance(values[prop_name][k], dict):
            raise TypeError(f'{prop_name}->{k}. invalid type. must be dict. {type(values[prop_name][k]).__name__} was given')

        combined_definition = dict(default)
        combined_definition.update(values[prop_name][k])
        values[prop_name][k] = combined_definition

        if 'name' not in values[prop_name][k]:
            values[prop_name][k]['name'] = k

        if 'id' not in values[prop_name][k]:
            values[prop_name][k]['id'] = uuid4()

        if '_extra_props_' in model_files:
            if '_extra_props_' in values[prop_name][k]:
                raise ValueError(f'{prop_name}->{k}->_extra_props_ must not be presented in specification')
            values[prop_name][k]['_extra_props_'] = dict()
            fields_to_pop = set()
            for kk in values[prop_name][k].keys():
                if kk not in model_files:
                    fields_to_pop.add(kk)
            for kk in fields_to_pop:
                values[prop_name][k]['_extra_props_'][kk] = values[prop_name][k].pop(kk)

        if _CONF_PROP in values[prop_name][k]:
            raise ValueError(f'{prop_name}->{k}->{_CONF_PROP} must not be specified in specification')

        values[prop_name][k][_CONF_PROP] = values
