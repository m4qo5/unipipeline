import logging
from time import sleep
from typing import Dict, List, Any, Tuple, Optional, NamedTuple

import yaml  # type: ignore
from crontab import CronTab  # type: ignore

from unipipeline.messages.uni_cron_message import UniCronMessage
from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_broker_definition import UniBrokerDefinition, UniMessageCodec, UniBrokerRMQPropsDefinition, UniBrokerKafkaPropsDefinition
from unipipeline.modules.uni_cron_task_definition import UniCronTaskDefinition
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


UNI_CRON_MESSAGE = "uni_cron_message"


logger = logging.getLogger(__name__)


class CronJob(NamedTuple):
    id: int
    name: str
    crontab: CronTab
    worker: UniWorker[UniCronMessage]
    message: UniCronMessage

    def new(self, id: int, task_def: UniCronTaskDefinition, worker: UniWorker[UniCronMessage]) -> 'CronJob':
        return CronJob(
            id=id,
            name=task_def.name,
            crontab=CronTab(task_def.when),
            worker=worker,
            message=UniCronMessage(
                task_name=task_def.name
            )
        )

    @staticmethod
    def search_next_tasks(all_tasks: List['CronJob']) -> Tuple[float, List['CronJob']]:
        min_delay: Optional[int] = None
        notification_list: List[CronJob] = []
        for cj in all_tasks:
            sec = cj.crontab.next(default_utc=False)
            if min_delay is None:
                min_delay = sec
            if sec < min_delay:
                notification_list.clear()
                min_delay = sec
            if sec <= min_delay:
                notification_list.append(cj)

        return min_delay, notification_list

    def send(self) -> None:
        self.worker.send(self.message)


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
            cron_tasks=self._parse_cron_tasks(),
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

    def start_cron(self) -> None:
        cron_jobs: List[CronJob] = dict()
        w_index: Dict[str, UniWorker] = dict()
        for i, task in enumerate(self._definition.cron_tasks.values()):
            if task.worker.name not in w_index:
                w_index[task.worker.name] = self.get_worker(task.worker.name)
            cron_jobs.append(CronJob.new(i, task, w_index[task.worker.name]))

        if len(cron_jobs) == 0:
            return

        while True:
            delay, tasks = CronJob.search_next_tasks(cron_jobs)

            if delay is None:
                return

            logger.info("sleep %s seconds before running the tasks: %s", delay, [cj.name for cj in tasks])

            sleep(delay)

            logger.info("run the tasks: %s", [cj.name for cj in tasks])

            for cj in tasks:
                cj.send()

    def get_worker(self, name: str) -> UniWorker[Any]:
        definition = self._definition.get_worker(name)
        worker_type = definition.type.import_class(UniWorker)
        w = worker_type(definition=definition, index=self._definition)
        return w

    def _parse_cron_tasks(self) -> Dict[str, UniCronTaskDefinition]:
        result = dict()
        workers = self._parse_workers()
        for name, definition in parse_definition(self._config.get("cron", dict()), dict()):
            w = workers[definition["worker"]]
            if w.input_message.name != UNI_CRON_MESSAGE:
                raise ValueError(f"input_message of worker '{w.name}' must be '{UNI_CRON_MESSAGE}'. '{w.input_message.name}' was given")
            result[name] = UniCronTaskDefinition(
                name=name,
                worker=w,
                when=definition["when"],
            )
        return result

    def _parse_messages(self) -> Dict[str, UniMessageTypeDefinition]:
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

        brokers = self._parse_brokers()
        messages = self._parse_messages()
        waitings = self._parse_waitings()

        for name, definition in parse_definition(self._config["workers"], defaults):
            assert isinstance(definition["output_workers"], list)
            output_workers = definition["output_workers"]

            for ow in definition["output_workers"]:
                assert isinstance(ow, str), f"ow must be str. {type(ow)} given"
                out_workers.add(ow)

            definition["broker"] = brokers[definition["broker"]]
            definition["input_message"] = messages[definition["input_message"]]

            watings: List[UniWaitingDefinition] = list()
            for w in definition["waiting_for"]:
                watings.append(waitings[w])

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
