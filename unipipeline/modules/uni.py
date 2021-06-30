import logging
from time import sleep
from typing import Dict, Any, Union

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
    def __init__(self, config: Union[UniConfig, str]) -> None:
        if isinstance(config, str):
            config = UniConfig(config)
        if not isinstance(config, UniConfig):
            raise ValueError(f'invalid config type. {type(config).__name__} was given')
        self._mediator = UniMediator(config)

    def check(self, create: bool = False) -> None:
        try:
            for broker_def in self._mediator.config.brokers.values():
                broker_def.type.import_class(UniBroker, create, create_template_params=broker_def)

            for message_def in self._mediator.config.messages.values():
                message_def.type.import_class(UniMessage, create, create_template_params=message_def)

            for worker_def in self._mediator.config.workers.values():
                worker_def.type.import_class(UniWorker, create, create_template_params=worker_def)

            for waiting_def in self._mediator.config.waitings.values():
                waiting_def.type.import_class(UniWaiting, create, create_template_params=waiting_def)

        except (ParseDefinitionError, UniConfigError) as e:
            print(f"ERROR: {e}")
            exit(1)

    def start_cron(self) -> None:
        cron_jobs = UniCronJob.mk_jobs_list(self._mediator.config.cron_tasks.values(), self._mediator)

        logger.debug(f'cron jobs defined: {", ".join(cj.task.name for cj in cron_jobs)}')

        while True:
            delay, jobs = UniCronJob.search_next_tasks(cron_jobs)

            if delay is None:
                return

            logger.debug("sleep %s seconds before running the tasks: %s", delay, [cj.task.name for cj in jobs])

            if delay > 0:
                sleep(delay)

            logger.info("run the tasks: %s", [cj.task.name for cj in jobs])

            for cj in jobs:
                cj.send()

            sleep(1.1)  # delay for correct next iteration

    def initialize(self, everything: bool = False, create: bool = True) -> None:
        if everything:
            for wn in self._mediator.config.workers.keys():
                self._mediator.add_worker_to_init_list(wn, no_related=True)
        self._mediator.initialize(create=create)

    def init_cron(self) -> None:
        for task in self._mediator.config.cron_tasks.values():
            self._mediator.add_worker_to_init_list(task.worker.name, no_related=True)

    def init_producer_worker(self, name: str) -> None:
        self._mediator.add_worker_to_init_list(name, no_related=True)

    def init_consumer_worker(self, name: str) -> None:
        self._mediator.add_worker_to_init_list(name, no_related=False)
        self._mediator.add_worker_to_consume_list(name)

    def send_to(self, name: str, data: Union[Dict[str, Any], UniMessage], alone: bool = False) -> None:
        self._mediator.send_to(name, data, alone=alone)

    def start_consuming(self) -> None:
        self._mediator.start_consuming()
