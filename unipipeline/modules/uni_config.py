from typing import Dict, Any, List

import yaml  # type: ignore

from unipipeline.modules.uni_worker_definition import UniWorkerDefinition
from unipipeline.modules.uni_message_type_definition import UniMessageTypeDefinition
from unipipeline.modules.uni_waiting_definition import UniWaitingDefinition
from unipipeline.modules.uni_broker_definition import  UniBrokerDefinition
from unipipeline.modules.uni_broker_definition import UniMessageCodec
from unipipeline.modules.uni_broker_definition import UniBrokerKafkaPropsDefinition
from unipipeline.modules.uni_broker_definition import UniBrokerRMQPropsDefinition
from unipipeline.modules.uni_cron_task_definition import UniCronTaskDefinition
from unipipeline.utils.parse_definition import parse_definition
from unipipeline.utils.parse_type import parse_type
from unipipeline.utils.template import template

UNI_CRON_MESSAGE = "uni_cron_message"


class ConfigError(Exception):
    pass


class UniConfig:
    def __init__(self, file_path: str) -> None:
        self._file_path = file_path

        self._config: Dict[str, Any] = dict()
        self._parsed = False
        self._config_loaded = False
        self._waitings_index: Dict[str, UniWaitingDefinition] = dict()
        self._brokers_index: Dict[str, UniBrokerDefinition] = dict()
        self._messages_index: Dict[str, UniMessageTypeDefinition] = dict()
        self._workers_index: Dict[str, UniWorkerDefinition] = dict()
        self._cron_tasks_index: Dict[str, UniCronTaskDefinition] = dict()

    @property
    def brokers(self) -> Dict[str, UniBrokerDefinition[Any]]:
        self._parse()
        return self._brokers_index

    @property
    def cron_tasks(self) -> Dict[str, UniCronTaskDefinition]:
        self._parse()
        return self._cron_tasks_index

    @property
    def workers(self) -> Dict[str, UniWorkerDefinition]:
        self._parse()
        return self._workers_index

    @property
    def waitings(self) -> Dict[str, UniWaitingDefinition]:
        self._parse()
        return self._waitings_index

    @property
    def messages(self) -> Dict[str, UniMessageTypeDefinition]:
        self._parse()
        return self._messages_index

    def _load_config(self) -> Dict[str, Any]:
        if self._config_loaded:
            return self._config
        self._config_loaded = True
        with open(self._file_path, "rt") as f:
            self._config = yaml.safe_load(f)
        if not isinstance(self._config, dict):
            raise ConfigError('config must be dict')
        return self._config

    def _parse(self) -> None:
        if self._parsed:
            return

        self._waitings_index = self._parse_waitings(self._load_config())
        self._brokers_index = self._parse_brokers(self._load_config())
        self._messages_index = self._parse_messages(self._load_config())
        self._workers_index = self._parse_workers(self._load_config(), self._brokers_index, self._messages_index, self._waitings_index)
        self._cron_tasks_index = self._parse_cron_tasks(self._load_config(), self._workers_index)

        self._parsed = True

    def _parse_cron_tasks(self, config: Dict[str, Any], workers: Dict[str, UniWorkerDefinition]) -> Dict[str, UniCronTaskDefinition]:
        result = dict()
        for name, definition in parse_definition("cron", config.get("cron", dict()), dict(), {"when", "worker"}):
            w = workers[definition["worker"]]
            if w.input_message.name != UNI_CRON_MESSAGE:
                raise ValueError(f"input_message of worker '{w.name}' must be '{UNI_CRON_MESSAGE}'. '{w.input_message.name}' was given")
            result[name] = UniCronTaskDefinition(
                name=name,
                worker=w,
                when=definition["when"],
            )
        return result

    def _parse_messages(self, config: Dict[str, Any]) -> Dict[str, UniMessageTypeDefinition]:
        from unipipeline import UniModuleDefinition

        result = {
            UNI_CRON_MESSAGE: UniMessageTypeDefinition(
                name=UNI_CRON_MESSAGE,
                type=UniModuleDefinition(
                    module="unipipeline.messages.uni_cron_message",
                    class_name="UniCronMessage",
                ),
            )
        }

        for name, definition in parse_definition("messages", config["messages"], dict(), {"import_template", }):
            import_template = definition.pop("import_template")
            result[name] = UniMessageTypeDefinition(
                **definition,
                type=parse_type(template(import_template, definition)),
            )

        return result

    def _parse_waitings(self, config: Dict[str, Any]) -> Dict[str, UniWaitingDefinition]:
        result = dict()
        defaults = dict(
            retry_max_count=3,
            retry_delay_s=10,
        )
        for name, definition in parse_definition('waitings', config['waitings'], defaults, {"import_template", }):
            result[name] = UniWaitingDefinition(
                **definition,
                type=parse_type(template(definition["import_template"], definition)),
            )

        return result

    def _parse_brokers(self, config: Dict[str, Any]) -> Dict[str, UniBrokerDefinition[Any]]:
        result: Dict[str, UniBrokerDefinition] = dict()
        defaults = dict(
            retry_max_count=3,
            retry_delay_s=10,

            content_type="application/json",
            compression=None,

            exchange_name="communication",
            exchange_type="direct",

            heartbeat=600,
            blocked_connection_timeout=300,
            socket_timeout=300,
            stack_timeout=300,
            passive=False,
            durable=True,
            auto_delete=False,
            is_persistent=True,
            api_version=[0, 10],
        )
        for name, definition in parse_definition("brokers", config["brokers"], defaults, {"import_template", }):
            result[name] = UniBrokerDefinition(
                **definition,
                type=parse_type(template(definition["import_template"], definition)),
                message_codec=UniMessageCodec(
                    content_type=definition["content_type"],
                    compression=definition["compression"],
                ),
                rmq_definition=UniBrokerRMQPropsDefinition(
                    exchange_name=definition['exchange_name'],
                    heartbeat=definition['heartbeat'],
                    blocked_connection_timeout=definition['blocked_connection_timeout'],
                    socket_timeout=definition['socket_timeout'],
                    stack_timeout=definition['stack_timeout'],
                    exchange_type=definition['exchange_type'],
                ),
                kafka_definition=UniBrokerKafkaPropsDefinition(
                    api_version=definition['api_version'],
                )
            )
        return result

    def _parse_workers(
        self,
        config: Dict[str, Any],
        brokers: Dict[str, UniBrokerDefinition],
        messages: Dict[str, UniMessageTypeDefinition],
        waitings: Dict[str, UniWaitingDefinition]
    ) -> Dict[str, UniWorkerDefinition]:
        result = dict()

        out_workers = set()

        defaults = dict(
            topic="{{name}}__{{input_message.name}}",
            broker="default_broker",
            prefetch=1,
            retry_max_count=3,
            retry_delay_s=1,
            max_ttl_s=None,
            is_permanent=True,
            ack_after_success=True,
            waiting_for=[],
            output_workers=[],
        )

        for name, definition in parse_definition("workers", config["workers"], defaults, {"import_template", "input_message", "broker"}):
            for ow in definition["output_workers"]:
                out_workers.add(ow)

            br = definition["broker"]
            if br not in brokers:
                raise ConfigError(f'definition workers->{name} has invalid broker: {br}')
            definition["broker"] = brokers[br]

            im = definition["input_message"]
            if im not in messages:
                raise ConfigError(f'definition workers->{name} has invalid input_message: {im}')
            definition["input_message"] = messages[im]

            waitings_: List[UniWaitingDefinition] = list()
            for w in definition["waiting_for"]:
                if w not in waitings:
                    raise ConfigError(f'definition workers->{name} has invalid waiting_for: {w}')
                waitings_.append(waitings[w])

            definition.update(
                type=parse_type(template(definition["import_template"], definition)),
                topic=template(definition["topic"], definition),
                waitings=waitings_,
            )

            defn = UniWorkerDefinition(**definition)

            result[name] = defn

        out_intersection_workers = set(result.keys()).intersection(out_workers)
        if len(out_intersection_workers) != len(out_workers):
            raise ConfigError(f'workers definition has invalid worker_names (in output_workers prop): {", ".join(out_intersection_workers)}')

        return result
