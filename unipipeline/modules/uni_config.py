from typing import Dict, Any, Set, Union, Type, Optional
from uuid import uuid4

import yaml  # type: ignore

from unipipeline.modules.uni_broker_definition import UniBrokerDefinition
from unipipeline.modules.uni_cron_task_definition import UniCronTaskDefinition
from unipipeline.modules.uni_echo import UniEcho
from unipipeline.modules.uni_external_definition import UniExternalDefinition
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_codec import UniMessageCodec
from unipipeline.modules.uni_message_definition import UniMessageDefinition
from unipipeline.modules.uni_module_definition import UniModuleDefinition
from unipipeline.modules.uni_service_definition import UniServiceDefinition
from unipipeline.modules.uni_waiting_definition import UniWaitingDefinition
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition
from unipipeline.utils.parse_definition import parse_definition
from unipipeline.utils.serializer_registry import CONTENT_TYPE__APPLICATION_JSON
from unipipeline.utils.template import template

UNI_CRON_MESSAGE = "uni_cron_message"


class UniConfigError(Exception):
    pass


class UniConfig:
    def __init__(self, file_path: str, echo_level: Optional[Union[str, int]] = None) -> None:
        self._file_path = file_path
        self._echo_level = echo_level
        self._echo = UniEcho('UNI', level=echo_level or 'error', colors=False)

        self._config: Dict[str, Any] = dict()
        self._parsed = False
        self._config_loaded = False
        self._waiting_index: Dict[str, UniWaitingDefinition] = dict()
        self._external: Dict[str, UniExternalDefinition] = dict()
        self._brokers_index: Dict[str, UniBrokerDefinition] = dict()
        self._messages_index: Dict[str, UniMessageDefinition] = dict()
        self._workers_by_name_index: Dict[str, UniWorkerDefinition] = dict()
        self._workers_by_class_index: Dict[str, UniWorkerDefinition] = dict()
        self._cron_tasks_index: Dict[str, UniCronTaskDefinition] = dict()
        self._service: UniServiceDefinition = None  # type: ignore

    @property
    def echo(self) -> UniEcho:
        return self._echo

    @property
    def file(self) -> str:
        return self._file_path

    @property
    def brokers(self) -> Dict[str, UniBrokerDefinition]:
        self._parse()
        return self._brokers_index

    @property
    def service(self) -> UniServiceDefinition:
        self._parse()
        return self._service

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

    def get_worker_definition(self, worker: Union[Type['UniWorker[UniMessage]'], str]) -> UniWorkerDefinition:
        if isinstance(worker, str):
            return self.workers[worker]
        return self.workers_by_class[worker.__name__]

    def _load_config(self) -> Dict[str, Any]:
        if self._config_loaded:
            return self._config
        self._config_loaded = True
        with open(self._file_path, "rt") as f:
            self._config = yaml.safe_load(f)
        if not isinstance(self._config, dict):
            raise UniConfigError('config must be dict')
        return self._config

    def _parse(self) -> None:
        if self._parsed:
            return
        self._parsed = True

        cfg = self._load_config()

        self._service = self._parse_service(cfg)
        self.echo.log_info(f'parsed service: {self._service.name}')

        self._external = self._parse_external_services(cfg)
        self.echo.log_info(f'parsed external: {",".join(self._external.keys())}')

        self._waiting_index = self._parse_waitings(cfg, self._service)
        self.echo.log_info(f'parsed waitings: {",".join(self._waiting_index.keys())}')

        self._brokers_index = self._parse_brokers(cfg, self._service, self._external)
        self.echo.log_info(f'parsed brokers: {",".join(self._brokers_index.keys())}')

        self._messages_index = self._parse_messages(cfg, self._service)
        self.echo.log_info(f'parsed messages: {",".join(self._messages_index.keys())}')

        self._workers_by_name_index = self._parse_workers(cfg, self._service, self._brokers_index, self._messages_index, self._waiting_index, self._external)
        self.echo.log_info(f'parsed workers: {",".join(self._workers_by_name_index.keys())}')

        for wd in self._workers_by_class_index.values():
            if wd.marked_as_external:
                continue
            assert wd.type is not None
            self._workers_by_class_index[wd.type.class_name] = wd

        self._cron_tasks_index = self._parse_cron_tasks(cfg, self._service, self._workers_by_name_index)

    def _parse_cron_tasks(self, config: Dict[str, Any], service: UniServiceDefinition, workers: Dict[str, UniWorkerDefinition]) -> Dict[str, UniCronTaskDefinition]:
        result = dict()
        defaults = dict(
            alone=True,
        )
        for name, definition, other_props in parse_definition("cron", config.get("cron", dict()), defaults, {"when", "worker"}):
            worker_def = workers[definition["worker"]]
            if worker_def.input_message.name != UNI_CRON_MESSAGE:
                raise ValueError(f"input_message of worker '{worker_def.name}' must be '{UNI_CRON_MESSAGE}'. '{worker_def.input_message.name}' was given")
            result[name] = UniCronTaskDefinition(
                id=definition["id"],
                name=name,
                worker=worker_def,
                when=definition["when"],
                alone=definition["alone"],
                dynamic_props_=other_props,
            )
        return result

    def _parse_messages(self, config: Dict[str, Any], service: UniServiceDefinition) -> Dict[str, UniMessageDefinition]:
        result = {
            UNI_CRON_MESSAGE: UniMessageDefinition(
                name=UNI_CRON_MESSAGE,
                type=UniModuleDefinition(
                    module="unipipeline.messages.uni_cron_message",
                    class_name="UniCronMessage",
                ),
                dynamic_props_=dict(),
            )
        }

        if "messages" not in config:
            raise UniConfigError(f'messages is not defined in config')

        for name, definition, other_props in parse_definition("messages", config["messages"], dict(), {"import_template", }):
            import_template = definition.pop("import_template")
            id_ = definition.pop("id")
            result[name] = UniMessageDefinition(
                **definition,
                type=UniModuleDefinition.parse(template(import_template, **definition, **{"service": service, "id": id_})),
                dynamic_props_=other_props,
            )

        return result

    def _parse_waitings(self, config: Dict[str, Any], service: UniServiceDefinition) -> Dict[str, UniWaitingDefinition]:
        result = dict()
        defaults = dict(
            retry_max_count=3,
            retry_delay_s=10,
        )

        for name, definition, other_props in parse_definition('waitings', config.get('waitings', dict()), defaults, {"import_template", }):
            result[name] = UniWaitingDefinition(
                **definition,
                type=UniModuleDefinition.parse(template(definition["import_template"], **definition, **{"service": service})),
                dynamic_props_=other_props
            )

        return result

    def _parse_brokers(self, config: Dict[str, Any], service: UniServiceDefinition, external: Dict[str, UniExternalDefinition]) -> Dict[str, UniBrokerDefinition]:
        result: Dict[str, UniBrokerDefinition] = dict()
        defaults = dict(
            retry_max_count=3,
            retry_delay_s=10,

            content_type=CONTENT_TYPE__APPLICATION_JSON,
            compression=None,
            external=None,
        )

        if "brokers" not in config:
            raise UniConfigError(f'brokers is not defined in config')

        for name, definition, other_def in parse_definition("brokers", config["brokers"], defaults, {"import_template", }):
            ext = definition["external"]
            if ext is not None and ext not in external:
                raise UniConfigError(f'definition brokers->{name} has invalid external: "{ext}"')

            result[name] = UniBrokerDefinition(
                **definition,
                type=UniModuleDefinition.parse(template(definition["import_template"], **definition, **{"service": service})),
                codec=UniMessageCodec(
                    content_type=definition["content_type"],
                    compression=definition["compression"],
                ),
                dynamic_props_=other_def,
            )
        return result

    def _parse_service(self, config: Dict[str, Any]) -> UniServiceDefinition:
        if "service" not in config:
            raise UniConfigError(f'service is not defined in config')

        service_conf = config["service"]

        if "name" not in service_conf:
            raise UniConfigError(f'service->name is not defined')

        clrs = service_conf.get('echo_colors', True)
        lvl = service_conf.get('echo_level', 'warning')

        self._echo = UniEcho('UNI', level=self._echo_level or lvl, colors=clrs)

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

        for name, definition, other_props in parse_definition('external', external_conf, defaults, set()):
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
            max_ttl_s=None,
            is_permanent=True,

            retry_max_count=3,
            retry_delay_s=1,
            topic="{{name}}",
            error_payload_topic="{{topic}}__error__payload",
            error_topic="{{topic}}__error",
            broker="default_broker",
            external=None,

            # notification_file="/var/unipipeline/{{service.name}}/{{service.id}}/worker_{{name}}_{{id}}/metrics",

            ack_after_success=True,
            waiting_for=[],
            output_workers=[],
        )

        if "workers" not in config:
            raise UniConfigError(f'workers is not defined in config')

        for name, definition, other_props in parse_definition("workers", config["workers"], defaults, {"import_template", "input_message"}):
            for ow in definition["output_workers"]:
                out_workers.add(ow)

            br = definition["broker"]
            if br not in brokers:
                raise UniConfigError(f'definition workers->{name} has invalid broker: {br}')
            definition["broker"] = brokers[br]

            im = definition["input_message"]
            if im not in messages:
                raise UniConfigError(f'definition workers->{name} has invalid input_message: {im}')
            definition["input_message"] = messages[im]

            ext = definition["external"]
            if ext is not None and ext not in external:
                raise UniConfigError(f'definition workers->{name} has invalid external: "{ext}"')

            waitings_: Set[UniWaitingDefinition] = set()
            for w in definition["waiting_for"]:
                if w not in waitings:
                    raise UniConfigError(f'definition workers->{name} has invalid waiting_for: {w}')
                waitings_.add(waitings[w])

            error_topic_template = definition.pop('error_topic')
            error_payload_topic_template = definition.pop('error_payload_topic')

            template_data: Dict[str, Any] = {**definition, "service": service}
            topic = template(definition.pop('topic'), **template_data)
            template_data['topic'] = topic

            defn = UniWorkerDefinition(
                **definition,
                type=UniModuleDefinition.parse(template(definition["import_template"], **template_data)) if definition["external"] is None else None,
                topic=topic,
                error_topic=template(error_topic_template, **template_data),
                error_payload_topic=template(error_payload_topic_template, **template_data),
                waitings=waitings_,
                dynamic_props_=other_props,
            )

            result[name] = defn

        out_intersection_workers = set(result.keys()).intersection(out_workers)
        if len(out_intersection_workers) != len(out_workers):
            raise UniConfigError(f'workers definition has invalid worker_names (in output_workers prop): {", ".join(out_intersection_workers)}')

        return result
