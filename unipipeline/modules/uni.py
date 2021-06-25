import logging
from time import sleep

from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_config import UniConfig, ConfigError
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
            for b in self._config.brokers.values():
                b.type.import_class(UniBroker, create, create_template_params=b)

            for m in self._config.messages.values():
                m.type.import_class(UniMessage, create, create_template_params=m)

            for worker in self._config.workers.values():
                worker.type.import_class(UniWorker, create, create_template_params=worker)

            for waiting in self._config.waitings.values():
                waiting.type.import_class(UniWaiting, create, create_template_params=waiting)
        except (ParseDefinitionError, ConfigError) as e:
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

    def get_worker(self, name: str, singleton: bool = True) -> UniWorker[UniMessage]:
        return self._mediator.get_worker(name, singleton)
