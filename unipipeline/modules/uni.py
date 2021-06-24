from typing import Dict, List, Any

import yaml  # type: ignore

from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_broker_definition import UniBrokerDefinition, UniMessageCodec, UniBrokerRMQPropsDefinition, UniBrokerKafkaPropsDefinition
from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_type_definition import UniMessageTypeDefinition
from unipipeline.modules.uni_waiting_definition import UniWaitingDefinition
from unipipeline.modules.uni_wating import UniWaiting
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition
from unipipeline.utils.parse_definition import parse_definition
from unipipeline.utils.parse_type import parse_type
from unipipeline.utils.template import template


class Uni:
    def __init__(self, config_file_path: str) -> None:
        self._config_file_path = config_file_path

        with open(config_file_path, "rt") as f:
            self._config = yaml.safe_load(f)

        self._definition = UniDefinition(
            waitings=self._parse_waitings(),
            brokers=self._parse_brokers(),
            messages=self._parse_messages(),
            workers=self._parse_workers(),
        )

    def check_load_all(self, create: bool = False) -> None:
        for b in self._definition.brokers.values():
            b.type.import_class(UniBroker, create, create_template_params=b)

        for m in self._definition.messages.values():
            m.type.import_class(UniMessage, create, create_template_params=m)

        for worker in self._definition.workers.values():
            worker.type.import_class(UniWorker, create, create_template_params=worker)

        for waiting in self._definition.waitings.values():
            waiting.type.import_class(UniWaiting, create, create_template_params=waiting)

    def get_worker(self, name: str) -> UniWorker[Any]:
        definition = self._definition.get_worker(name)
        worker_type = definition.type.import_class(UniWorker)
        w = worker_type(definition=definition, index=self._definition)
        return w

    def _parse_messages(self) -> Dict[str, UniMessageTypeDefinition]:
        result = dict()
        for name, definition in parse_definition(self._config["messages"], dict()):
            import_template = definition.pop("import_template")
            result[name] = UniMessageTypeDefinition(
                **definition,
                type=parse_type(template(import_template, definition)),
            )
        return result

    def _parse_waitings(self) -> Dict[str, UniWaitingDefinition]:
        result = dict()
        defaults = dict(
            retry_max_count=3,
            retry_delay_s=10,
        )
        for name, definition in parse_definition(self._config['waitings'], defaults):
            result[name] = UniWaitingDefinition(
                **definition,
                type=parse_type(template(definition["import_template"], definition)),
            )
        return result

    def _parse_brokers(self) -> Dict[str, UniBrokerDefinition[Any]]:
        result: Dict[str, Any] = dict()
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
        for name, definition in parse_definition(self._config["brokers"], defaults):
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

    def _parse_workers(self) -> Dict[str, UniWorkerDefinition]:
        result = dict()

        out_workers = set()

        defaults = dict(
            output_workers=[],
            topic="{{name}}__{{input_message.name}}",
            broker="default_broker",
            prefetch=1,
            retry_max_count=3,
            retry_delay_s=1,
            max_ttl_s=None,
            is_permanent=True,
            auto_ack=True,
            waiting_for=[],
        )

        for name, definition in parse_definition(self._config["workers"], defaults):
            assert isinstance(definition["output_workers"], list)
            output_workers = definition["output_workers"]

            for ow in definition["output_workers"]:
                assert isinstance(ow, str), f"ow must be str. {type(ow)} given"
                out_workers.add(ow)

            definition["broker"] = self._parse_brokers()[definition["broker"]]
            definition["input_message"] = self._parse_messages()[definition["input_message"]]

            watings: List[UniWaitingDefinition] = list()
            for w in definition["waiting_for"]:
                watings.append(self._parse_waitings()[w])

            definition.update(dict(
                type=parse_type(template(definition["import_template"], definition)),
                topic=template(definition["topic"], definition),
                output_workers=output_workers,
                waitings=watings,
            ))

            defn = UniWorkerDefinition(**definition)

            result[name] = defn

        assert set(result.keys()).intersection(out_workers), f"invalid workers relations: {out_workers.difference(set(result.keys()))}"

        return result
