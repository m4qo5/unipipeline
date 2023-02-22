from typing import Dict, Any, Set, Union, Type, Iterator, Tuple, Mapping, Optional
from uuid import uuid4

import yaml
from pydantic import BaseModel

from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.definitions.uni_codec_definition import UniCodecDefinition
from unipipeline.definitions.uni_cron_task_definition import UniCronTaskDefinition
from unipipeline.definitions.uni_external_definition import UniExternalDefinition
from unipipeline.definitions.uni_message_definition import UniMessageDefinition
from unipipeline.definitions.uni_module_definition import UniModuleDefinition
from unipipeline.definitions.uni_service_definition import UniServiceDefinition
from unipipeline.definitions.uni_waiting_definition import UniWaitingDefinition
from unipipeline.definitions.uni_worker_definition import UniWorkerDefinition
from unipipeline.errors import UniConfigError, UniDefinitionNotFoundError
from unipipeline.utils.uni_echo import UniEcho
from unipipeline.utils.uni_util import UniUtil
from unipipeline.worker.uni_worker import UniWorker

UNI_CRON_MESSAGE = "uni_cron_message"


class UniConfig(BaseModel):
    @staticmethod
    def from_file(file_path: str) -> 'UniConfig':
        with open(file_path, "rt") as f:
            config_data = yaml.safe_load(f)
        return UniConfig(**config_data)

    compression: Dict[str, UniCodecDefinition] = dict()
    codecs: Dict[str, UniCodecDefinition] = dict()
    waitings: Dict[str, UniWaitingDefinition] = dict()
    external: Dict[str, UniExternalDefinition] = dict()
    brokers: Dict[str, UniBrokerDefinition] = dict()
    messages: Dict[str, UniMessageDefinition] = dict()
    workers: Dict[str, UniWorkerDefinition] = dict()
    cron_tasks: Dict[str, UniCronTaskDefinition] = dict()
    service: UniServiceDefinition

    _workers_by_class: Optional[Dict[str, UniWorkerDefinition]] = None  # TODO

    def _init(self, util: UniUtil, echo: UniEcho) -> None:
        assert self._workers_by_class is None
        if not self.workers:
            raise UniConfigError('workers not found')

        echo.log_info(f'service: {self.service.name}')
        echo.log_info(f'compression codecs: {",".join(self.compression.keys())}')
        echo.log_info(f'serialization codecs: {",".join(self.codecs.keys())}')
        echo.log_info(f'external: {",".join(self.external.keys())}')
        echo.log_info(f'waitings: {",".join(self.waitings.keys())}')
        echo.log_info(f'brokers: {",".join(self.brokers.keys())}')
        echo.log_info(f'messages: {",".join(self.messages.keys())}')
        echo.log_info(f'workers: {",".join(self.workers.keys())}')

        self._workers_by_class = dict()
        for wd in self.workers.values():
            if not wd.marked_as_external:
                assert wd.import_template is not None
                self._workers_by_class[wd.import_template.object_name] = wd

    @property
    def workers_by_class(self) -> Mapping[str, UniWorkerDefinition]:
        assert self._workers_by_class is not None
        return self._workers_by_class

    def get_worker_definition(self, worker: Union[Type['UniWorker[Any, Any]'], str]) -> UniWorkerDefinition:
        if isinstance(worker, str):
            if worker in self.workers:
                return self.workers[worker]
            else:
                raise UniDefinitionNotFoundError(f'worker {worker} is not defined in workers')
        elif issubclass(worker, UniWorker):
            if worker.__name__ in self.workers_by_class:
                return self.workers_by_class[worker.__name__]
            else:
                raise UniDefinitionNotFoundError(f'worker {worker.__name__} is not defined in workers')
        raise UniDefinitionNotFoundError(f'invalid type of worker. must be subclass of UniWorker OR str name. "{type(worker).__name__}" was given')

    def _parse_definition(
        self,
        conf_name: str,
        definitions: Dict[str, Any],
        defaults: Dict[str, Any],
        required_keys: Set[str]
    ) -> Iterator[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
        if not isinstance(definitions, dict):
            raise UniConfigError(f'worker_definition of {conf_name} has invalid type. must be dict')

        common = definitions.get("__default__", dict())

        for name, raw_definition in definitions.items():
            other_props: Dict[str, Any] = dict()

            name = str(name)
            if name.startswith("_"):
                if name == "__default__":
                    continue
                else:
                    raise ValueError(f"key '{name}' is not acceptable")

            if not isinstance(raw_definition, dict):
                raise UniConfigError(f'worker_definition of {conf_name}->{name} has invalid type. must be dict')

            result_definition = dict(defaults)
            combined_definition = dict(common)
            combined_definition.update(raw_definition)

            for k, v in combined_definition.items():
                if k in defaults:
                    result_definition[k] = v
                    vd = defaults.get(k, None)
                    if vd is not None and type(vd) != type(v):
                        raise UniConfigError(f'worker_definition of {conf_name}->{name} has invalid key "{k}" type')
                elif k in required_keys:
                    result_definition[k] = v
                else:
                    other_props[k] = v

            for rk in required_keys:
                if rk not in result_definition:
                    raise UniConfigError(f'worker_definition of {conf_name}->{name} has no required prop "{rk}"')

            result_definition["name"] = name
            result_definition["id"] = uuid4()

            yield name, result_definition, other_props

    APPLICATION_JSON: str = 'application/json'
    COMPRESSION_GZIP: str = "application/x-gzip"
    COMPRESSION_BZ2: str = "application/x-bz2"
    COMPRESSION_LZMA: str = "application/x-lzma"

    def _parse_compression(self, config: Dict[str, Any]) -> Dict[str, UniCodecDefinition]:
        result = {
            self.COMPRESSION_GZIP: UniCodecDefinition(
                name=self.COMPRESSION_GZIP,
                encoder_type=UniModuleDefinition.parse("zlib:compress"),
                decoder_type=UniModuleDefinition.parse("zlib:decompress"),
                dynamic_props_={}
            ),
            self.COMPRESSION_BZ2: UniCodecDefinition(
                name=self.COMPRESSION_BZ2,
                encoder_type=UniModuleDefinition.parse("bz2:compress"),
                decoder_type=UniModuleDefinition.parse("bz2:decompress"),
                dynamic_props_={}
            ),
            self.COMPRESSION_LZMA: UniCodecDefinition(
                name=self.COMPRESSION_LZMA,
                encoder_type=UniModuleDefinition.parse("lzma:compress"),
                decoder_type=UniModuleDefinition.parse("lzma:decompress"),
                dynamic_props_={}
            ),
        }

        for name, definition, other_props in self._parse_definition('compression', config.get("compression", dict()), dict(), {"encoder_import_template", "decoder_import_template"}):
            result[name] = UniCodecDefinition(
                name=name,
                encoder_type=UniModuleDefinition.parse(self._util.template.template(definition['encoder_import_template'], name=name)),
                decoder_type=UniModuleDefinition.parse(self._util.template.template(definition['decoder_import_template'], name=name)),
                dynamic_props_={}
            )

        return result

    def _parse_codecs(self, config: Dict[str, Any]) -> Dict[str, UniCodecDefinition]:
        result = {
            self.APPLICATION_JSON: UniCodecDefinition(
                name=self.APPLICATION_JSON,
                encoder_type=UniModuleDefinition.parse("unipipeline.utils.complex_serializer:complex_serializer_json_dumps"),
                decoder_type=UniModuleDefinition.parse("json:loads"),
                dynamic_props_={}
            ),
        }

        for name, definition, other_props in self._parse_definition('codecs', config.get("codecs", dict()), dict(), {"encoder_import_template", "decoder_import_template"}):
            result[name] = UniCodecDefinition(
                name=name,
                encoder_type=UniModuleDefinition.parse(self._util.template.template(definition['encoder_import_template'], name=name)),
                decoder_type=UniModuleDefinition.parse(self._util.template.template(definition['decoder_import_template'], name=name)),
                dynamic_props_={}
            )

        return result

    def _parse_cron_tasks(self, config: Dict[str, Any], service: UniServiceDefinition, workers: Dict[str, UniWorkerDefinition]) -> Dict[str, UniCronTaskDefinition]:
        result = dict()
        defaults = dict(
            alone=True,
            every_sec=None,
            when=None,
        )
        for name, definition, other_props in self._parse_definition("cron", config.get("cron", dict()), defaults, {"worker"}):
            worker_def = workers[definition["worker"]]
            if worker_def.input_message.name != UNI_CRON_MESSAGE:
                raise ValueError(f"input_message of worker '{worker_def.name}' must be '{UNI_CRON_MESSAGE}'. '{worker_def.input_message.name}' was given")
            result[name] = UniCronTaskDefinition(
                id=definition["id"],
                name=name,
                worker=worker_def,
                every_sec=definition.get('every_sec', None),
                when=definition.get('when', None),
                alone=definition["alone"],
                dynamic_props_=other_props,
            )
        return result

    def _parse_messages(self, config: Dict[str, Any], service: UniServiceDefinition) -> Dict[str, UniMessageDefinition]:
        result = {
            UNI_CRON_MESSAGE: UniMessageDefinition(
                name=UNI_CRON_MESSAGE,
                type=UniModuleDefinition(
                    module="unipipeline.message.uni_cron_message",
                    object_name="UniCronMessage",
                ),
                dynamic_props_=dict(),
            )
        }

        if "messages" not in config:
            raise UniConfigError('messages is not defined in config')

        for name, definition, other_props in self._parse_definition("messages", config["messages"], dict(), {"import_template", }):
            import_template = definition.pop("import_template")
            id_ = definition.pop("id")
            result[name] = UniMessageDefinition(
                **definition,
                type=UniModuleDefinition.parse(self._util.template.template(import_template, **definition, **{"service": service, "id": id_})),
                dynamic_props_=other_props,
            )

        return result

    def _parse_waitings(self, config: Dict[str, Any], service: UniServiceDefinition) -> Dict[str, UniWaitingDefinition]:
        result = dict()
        defaults = dict(
            retry_max_count=3,
            retry_delay_s=10,
        )

        for name, definition, other_props in self._parse_definition('waitings', config.get('waitings', dict()), defaults, {"import_template", }):
            import_template = definition.pop("import_template")
            result[name] = UniWaitingDefinition(
                **definition,
                type=UniModuleDefinition.parse(self._util.template.template(import_template, **definition, **{"service": service})),
                dynamic_props_=other_props
            )

        return result

    def _parse_brokers(self, config: Dict[str, Any], service: UniServiceDefinition, external: Dict[str, UniExternalDefinition]) -> Dict[str, UniBrokerDefinition]:
        result: Dict[str, UniBrokerDefinition] = dict()
        defaults = dict(
            retry_max_count=3,
            retry_delay_s=10,

            content_type=self.APPLICATION_JSON,
            compression=None,
            external=None,
        )

        if "brokers" not in config:
            raise UniConfigError('brokers is not defined in config')

        for name, definition, other_def in self._parse_definition("brokers", config["brokers"], defaults, {"import_template", }):
            ext = definition["external"]
            if ext is not None and ext not in external:
                raise UniConfigError(f'worker_definition brokers->{name} has invalid external: "{ext}"')

            import_template = definition.pop('import_template')
            result[name] = UniBrokerDefinition(
                **definition,
                type=UniModuleDefinition.parse(self._util.template.template(import_template, **definition, **{"service": service})),
                dynamic_props_=other_def,
            )
        return result

    def _parse_service(self, config: Dict[str, Any]) -> UniServiceDefinition:
        if "service" not in config:
            raise UniConfigError('service is not defined in config')

        service_conf = config["service"]

        if "name" not in service_conf:
            raise UniConfigError('service->name is not defined')

        clrs = service_conf.get('echo_colors', True)
        lvl = service_conf.get('echo_level', 'warning')

        self._echo.level = lvl  # TODO: enable verbose if set
        self._util.color.enabled = clrs

        return UniServiceDefinition(
            name=service_conf["name"],
            id=uuid4(),
            colors=clrs,
            echo_level=lvl,
        )

    def _parse_external_services(self, config: Dict[str, Any]) -> Dict[str, UniExternalDefinition]:
        if "external" not in config:
            return dict()

        external_conf = config["external"]

        defaults: Dict[str, Any] = dict()

        result = dict()

        for name, definition, other_props in self._parse_definition('external', external_conf, defaults, set()):
            if other_props:
                raise UniConfigError(f'external->{name} has invalid props: {set(other_props.keys())}')

            dfn = UniExternalDefinition(
                **definition,
                dynamic_props_=dict(),
            )
            result[name] = dfn

        return result

    def _parse_workers(
        self,
        config: Dict[str, Any],
        service: UniServiceDefinition,
        brokers: Dict[str, UniBrokerDefinition],
        messages: Dict[str, UniMessageDefinition],
        waitings: Dict[str, UniWaitingDefinition],
        external: Dict[str, UniExternalDefinition],
    ) -> Dict[str, UniWorkerDefinition]:
        result = dict()

        out_workers = set()

        defaults: Dict[str, Any] = dict(
            topic="{{name}}",
            error_payload_topic="{{topic}}__error__payload",
            error_topic="{{topic}}__error",
            answer_topic="{{name}}__answer",
            broker="default_broker",
            external=None,
            answer_message=None,
            prefetch_count=1,

            input_unwrapped=False,
            answer_unwrapped=False,

            # notification_file="/var/unipipeline/{{service.name}}/{{service.id}}/worker_{{name}}_{{id}}/metrics",

            waiting_for=[],
            output_workers=[],
        )

        for name, definition, other_props in self._parse_definition("workers", config["workers"], defaults, {"import_template", "input_message"}):
            for ow in definition["output_workers"]:
                out_workers.add(ow)

            br = definition["broker"]
            if br not in brokers:
                raise UniConfigError(f'worker_definition workers->{name} has invalid broker: {br}')
            definition["broker"] = brokers[br]

            im = definition["input_message"]
            if im not in messages:
                raise UniConfigError(f'worker_definition workers->{name} has invalid input_message: {im}')
            definition["input_message"] = messages[im]

            om = definition['answer_message']
            if om is not None:
                if om not in messages:
                    raise UniConfigError(f'worker_definition workers->{name} has invalid answer_message: {om}')
                definition["answer_message"] = messages[om]

            ext = definition["external"]
            if ext is not None and ext not in external:
                raise UniConfigError(f'worker_definition workers->{name} has invalid external: "{ext}"')

            waitings_: Set[UniWaitingDefinition] = set()
            for w in definition.pop('waiting_for'):
                if w not in waitings:
                    raise UniConfigError(f'worker_definition workers->{name} has invalid waiting_for: {w}')
                waitings_.add(waitings[w])

            topic_template = definition.pop('topic')
            error_topic_template = definition.pop('error_topic')
            error_payload_topic_template = definition.pop('error_payload_topic')
            answer_topic_template = definition.pop('answer_topic')

            topic_templates = {topic_template, error_topic_template, error_payload_topic_template, answer_topic_template}
            if len(topic_templates) != 4:
                raise UniConfigError(f'worker_definition workers->{name} has duplicate topic templates: {", ".join(topic_templates)}')

            template_data: Dict[str, Any] = {**definition, "service": service}
            topic = self._util.template.template(topic_template, **template_data)
            template_data['topic'] = topic

            import_template = definition.pop("import_template")

            defn = UniWorkerDefinition(
                **definition,
                type=UniModuleDefinition.parse(self._util.template.template(import_template, **template_data)) if definition["external"] is None else None,
                topic=topic,
                error_topic=self._util.template.template(error_topic_template, **template_data),
                error_payload_topic=self._util.template.template(error_payload_topic_template, **template_data),
                answer_topic=self._util.template.template(answer_topic_template, **template_data),
                waitings=waitings_,
                dynamic_props_=other_props,
            )

            result[name] = defn

        out_intersection_workers = set(result.keys()).intersection(out_workers)
        if len(out_intersection_workers) != len(out_workers):
            raise UniConfigError(f'workers worker_definition has invalid worker_names (in output_workers prop): {", ".join(out_intersection_workers)}')

        return result

    class Config:
        arbitrary_types_allowed = True
