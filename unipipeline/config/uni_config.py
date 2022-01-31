from typing import Dict, Any, Set, Union, Type, Iterator, Tuple
from uuid import uuid4

from unipipeline.config.uni_config_loader import UniConfigLoader
from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.definitions.uni_codec_definition import UniCodecDefinition
from unipipeline.definitions.uni_cron_task_definition import UniCronTaskDefinition
from unipipeline.definitions.uni_external_definition import UniExternalDefinition
from unipipeline.definitions.uni_message_definition import UniMessageDefinition
from unipipeline.definitions.uni_module_definition import UniModuleDefinition
from unipipeline.definitions.uni_service_definition import UniServiceDefinition
from unipipeline.definitions.uni_waiting_definition import UniWaitingDefinition
from unipipeline.definitions.uni_worker_definition import UniWorkerDefinition
from unipipeline.errors.uni_config_error import UniConfigError
from unipipeline.errors.uni_definition_not_found_error import UniDefinitionNotFoundError
from unipipeline.utils.uni_echo import UniEcho
from unipipeline.utils.uni_util import UniUtil
from unipipeline.worker.uni_worker import UniWorker

UNI_CRON_MESSAGE = "uni_cron_message"


class UniConfig:
    def __init__(self, util: UniUtil, echo: UniEcho, loader: UniConfigLoader) -> None:
        self._util = util
        self._loader = loader
        self._echo = echo

        self._config: Dict[str, Any] = dict()
        self._parsed = False
        self._config_loaded = False
        self._compression_index: Dict[str, UniCodecDefinition] = dict()
        self._codecs_index: Dict[str, UniCodecDefinition] = dict()
        self._waiting_index: Dict[str, UniWaitingDefinition] = dict()
        self._external: Dict[str, UniExternalDefinition] = dict()
        self._brokers_index: Dict[str, UniBrokerDefinition] = dict()
        self._messages_index: Dict[str, UniMessageDefinition] = dict()
        self._workers_by_name_index: Dict[str, UniWorkerDefinition] = dict()
        self._workers_by_class_index: Dict[str, UniWorkerDefinition] = dict()
        self._cron_tasks_index: Dict[str, UniCronTaskDefinition] = dict()
        self._service: UniServiceDefinition = None  # type: ignore

    @property
    def brokers(self) -> Dict[str, UniBrokerDefinition]:
        self._parse()
        return self._brokers_index

    @property
    def service(self) -> UniServiceDefinition:
        self._parse()
        return self._service

    @property
    def compression(self) -> Dict[str, UniCodecDefinition]:
        self._parse()
        return self._compression_index

    @property
    def codecs(self) -> Dict[str, UniCodecDefinition]:
        self._parse()
        return self._codecs_index

    @property
    def cron_tasks(self) -> Dict[str, UniCronTaskDefinition]:
        self._parse()
        return self._cron_tasks_index

    @property
    def external(self) -> Dict[str, UniExternalDefinition]:
        self._parse()
        return self._external

    @property
    def workers(self) -> Dict[str, UniWorkerDefinition]:
        self._parse()
        return self._workers_by_name_index

    @property
    def workers_by_class(self) -> Dict[str, UniWorkerDefinition]:
        self._parse()
        return self._workers_by_class_index

    @property
    def waitings(self) -> Dict[str, UniWaitingDefinition]:
        self._parse()
        return self._waiting_index

    @property
    def messages(self) -> Dict[str, UniMessageDefinition]:
        self._parse()
        return self._messages_index

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

    def _parse(self) -> None:
        if self._parsed:
            return
        self._parsed = True

        cfg = self._loader.load()

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
            answer_topic="{{name}}__answer",
            broker="default_broker",
            external=None,

            answer_message=None,
            answer_avg_delay_s=3,
            prefetch_count=1,

            input_unwrapped=False,
            answer_unwrapped=False,

            # notification_file="/var/unipipeline/{{service.name}}/{{service.id}}/worker_{{name}}_{{id}}/metrics",

            ack_after_success=True,
            waiting_for=[],
            output_workers=[],
        )

        if "workers" not in config:
            raise UniConfigError('workers is not defined in config')

        for name, definition, other_props in self._parse_definition("workers", config["workers"], defaults, {"import_template", "input_message"}):
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
