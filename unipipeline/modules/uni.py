import logging
from time import sleep
from typing import Dict, Any

from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_config import UniConfig, UniConfigError
from unipipeline.modules.uni_cron_job import UniCronJob
from unipipeline.modules.uni_mediator import UniMediator
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_wating import UniWaiting
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.utils.parse_definition import ParseDefinitionError

logger = logging.getLogger(__name__)


class Uni:
    def __init__(self, config_file_path: str) -> None:
        self._config = UniConfig(config_file_path)
        self._mediator = UniMediator(self._config)

    def check_load_all(self, create: bool = False) -> None:
        try:
            for broker_def in self._config.brokers.values():
                broker_def.type.import_class(UniBroker, create, create_template_params=broker_def)

            for message_def in self._config.messages.values():
                message_def.type.import_class(UniMessage, create, create_template_params=message_def)

            for worker_def in self._config.workers.values():
                worker_def.type.import_class(UniWorker, create, create_template_params=worker_def)

            for waiting_def in self._config.waitings.values():
                waiting_def.type.import_class(UniWaiting, create, create_template_params=waiting_def)

        except (ParseDefinitionError, UniConfigError) as e:
            print(f"ERROR: {e}")
            exit(1)

    def start_cron(self) -> None:
        cron_jobs = [UniCronJob.new(i, task, self.get_worker(task.worker.name)) for i, task in enumerate(self._config.cron_tasks.values())]

        if len(cron_jobs) == 0:
            return

        logger.info(f'cron jobs defined: {", ".join(cj.name for cj in cron_jobs)}')

        while True:
            delay, tasks = UniCronJob.search_next_tasks(cron_jobs)

            if delay is None:
                return

            logger.info("sleep %s seconds before running the tasks: %s", delay, [cj.name for cj in tasks])

            if delay > 0:
                sleep(delay)

            logger.info("run the tasks: %s", [cj.name for cj in tasks])

            for cj in tasks:
                cj.send()

            sleep(1.1)  # delay for correct next iteration

    @property
    def config(self) -> UniConfig:
        return self._config

    def get_worker(self, name: str, singleton: bool = True) -> UniWorker[UniMessage]:
        return self._mediator.get_worker(name, singleton)

    def initialize(self, create: bool = True) -> None:
        self._mediator.initialize(create=create)

    def consume(self, name: str) -> None:
        self._mediator.add_worker_to_init_list(name, no_related=False)
        self._mediator.add_worker_to_consume_list(name)

    def send_to_worker(self, name, data: Dict[str, Any]) -> None:
        self._mediator.add_worker_to_init_list(name, no_related=True)
        self._mediator.initialize()
        w = self._mediator.get_worker(name)
        w.send(data)

    def start_consuming(self) -> None:
        self._mediator.start_consuming()
