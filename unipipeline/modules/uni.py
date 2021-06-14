from typing import Dict, List, Any

import yaml

from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_broker_definition import UniBrokerDefinition, UniMessageCodec, UniBrokerRMQPropsDefinition, UniBrokerKafkaPropsDefinition
from unipipeline.modules.uni_definition import UniDefinition
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_message_type_definition import UniMessageTypeDefinition
from unipipeline.modules.uni_waiting_definition import UniWaitingDefinition
from unipipeline.modules.uni_wating import UniWaiting
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_worker_definition import UniWorkerDefinition, WORKER_ERROR_NAME
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

    def check_load_all(self) -> None:
        for b in self._definition.brokers.values():
            b.type.import_class(UniBroker)

        for m in self._definition.messages.values():
            m.type.import_class(UniMessage)

        for worker in self._definition.workers.values():
            worker.type.import_class(UniWorker)

        for waiting in self._definition.waitings.values():
            waiting.type.import_class(UniWaiting)

    def get_worker(self, name: str) -> UniWorker[Any]:
        definition = self._definition.get_worker(name)
        worker_type = definition.type.import_class(UniWorker)
        w = worker_type(definition=definition, index=self._definition)
        return w

    def _parse_messages(self) -> Dict[str, UniMessageTypeDefinition]:
        result = dict()
        for definition in parse_definition(self._config["messages"]):
            name = definition["name"]
            data = dict(
                name=name,
                version=definition["version"],
            )
            result[name] = UniMessageTypeDefinition(
                **data,
                type=parse_type(template(definition["type"], data)),
            )
        return result

    def _parse_waitings(self) -> Dict[str, UniWaitingDefinition]:
        result = dict()
        for definition in parse_definition(self._config['waitings']):
            name = definition["name"]
            data = dict(
                name=name,
                retry_max_count=definition["retry_max_count"],
                retry_delay_s=definition["retry_delay_s"],
            )
            result[name] = UniWaitingDefinition(
                **data,
                type=parse_type(template(definition["type"], data)),
            )
        return result

    def _parse_brokers(self) -> Dict[str, UniBrokerDefinition[Any]]:
        result: Dict[str, Any] = dict()
        for definition in parse_definition(self._config["brokers"]):
            name = definition["name"]
            data = dict(
                name=name,
                retry_max_count=definition["retry_max_count"],
                retry_delay_s=definition["retry_delay_s"],
                passive=definition["passive"],
                durable=definition["durable"],
                auto_delete=definition["auto_delete"],
                is_persistent=definition["is_persistent"],
            )

            result[name] = UniBrokerDefinition(
                **data,
                type=parse_type(template(definition["type"], data)),
                message_codec=UniMessageCodec(
                    content_type=definition["content_type"],
                    compression_content_type=definition["compression"],
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
                    bootstrap_servers=definition['bootstrap_servers'],
                    api_version=definition['api_version'],
                )

            )
        return result

    def _parse_workers(self) -> Dict[str, UniWorkerDefinition]:
        result = dict()

        out_workers = set()

        for definition in parse_definition(self._config["workers"], {WORKER_ERROR_NAME, }):
            name = definition["name"]

            assert isinstance(definition["output_workers"], list)
            output_workers = definition["output_workers"]

            for ow in definition["output_workers"]:
                assert isinstance(ow, str), f"ow must be str. {type(ow)} given"
                out_workers.add(ow)

            broker = self._parse_brokers()[definition["broker"]]
            input_message = self._parse_messages()[definition["input_message"]]

            watings: List[UniWaitingDefinition] = list()
            for w in definition["waiting_for"]:
                watings.append(self._parse_waitings()[w])

            data = dict(
                name=name,
                broker=broker,
                is_permanent=definition["is_permanent"],
                auto_ack=definition["auto_ack"],
                prefetch=definition["prefetch"],
                input_message=input_message,
                retry_max_count=definition["retry_max_count"],
                retry_delay_s=definition["retry_delay_s"],
                max_ttl_s=definition["max_ttl_s"],
            )

            defn = UniWorkerDefinition(
                type=parse_type(template(definition["type"], data)),
                topic=template(definition["topic"], data),
                output_workers=output_workers,
                waitings=watings,
                **data,
            )

            result[name] = defn

        assert set(result.keys()).intersection(out_workers), f"invalid workers relations: {out_workers.difference(set(result.keys()))}"

        return result
