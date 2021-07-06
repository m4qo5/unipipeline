from typing import Dict, Any, Union, Optional

from unipipeline.modules.uni_broker import UniBroker
from unipipeline.modules.uni_config import UniConfig, UniConfigError
from unipipeline.modules.uni_echo import UniEcho
from unipipeline.modules.uni_mediator import UniMediator, PayloadError
from unipipeline.modules.uni_message import UniMessage
from unipipeline.modules.uni_wating import UniWaiting
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.utils.parse_definition import ParseDefinitionError


class Uni:
    def __init__(self, config: Union[UniConfig, str], echo_level: Optional[Union[str, int]] = None) -> None:
        if isinstance(config, str):
            config = UniConfig(config, echo_level)
        if not isinstance(config, UniConfig):
            raise ValueError(f'invalid config type. {type(config).__name__} was given')
        self._mediator = UniMediator(config)

    @property
    def echo(self) -> UniEcho:
        return self._mediator.echo

    def set_echo_level(self, level: int):
        self._mediator.set_echo_level(level)

    def scaffold(self) -> None:
        try:
            for waiting_def in self._mediator.config.waitings.values():
                waiting_def.type.import_class(UniWaiting, self.echo, auto_create=True, create_template_params=waiting_def)

            for broker_def in self._mediator.config.brokers.values():
                broker_def.type.import_class(UniBroker, self.echo, auto_create=True, create_template_params=broker_def)

            for message_def in self._mediator.config.messages.values():
                message_def.type.import_class(UniMessage, self.echo, auto_create=True, create_template_params=message_def)

            for worker_def in self._mediator.config.workers.values():
                if worker_def.marked_as_external:
                    continue
                assert worker_def.type is not None
                worker_def.type.import_class(UniWorker, self.echo, auto_create=True, create_template_params=worker_def)

        except (ParseDefinitionError, UniConfigError) as e:
            self.echo.exit_with_error(str(e))

    def check(self) -> None:
        try:
            for waiting_def in self._mediator.config.waitings.values():
                waiting_def.type.import_class(UniWaiting, self.echo)

            for broker_def in self._mediator.config.brokers.values():
                broker_def.type.import_class(UniBroker, self.echo)

            for message_def in self._mediator.config.messages.values():
                message_def.type.import_class(UniMessage, self.echo)

            for worker_def in self._mediator.config.workers.values():
                if worker_def.marked_as_external:
                    continue
                assert worker_def.type is not None
                worker_def.type.import_class(UniWorker, self.echo)

        except (ParseDefinitionError, UniConfigError) as e:
            self.echo.exit_with_error(str(e))

    def start_cron(self) -> None:
        try:
            self._mediator.start_cron()
        except KeyboardInterrupt:
            self.echo.log_warning('interrupted')
            exit(0)

    def initialize_cron_workers(self) -> None:
        for t in self._mediator.config.cron_tasks.values():
            self.init_producer_worker(t.worker.name)

    def initialize(self, everything: bool = False) -> None:
        if everything:
            for wn in self._mediator.config.workers.keys():
                self._mediator.add_worker_to_init_list(wn, no_related=True)
        self._mediator.initialize(create=True)

    def init_cron(self) -> None:
        for task in self._mediator.config.cron_tasks.values():
            self._mediator.add_worker_to_init_list(task.worker.name, no_related=True)

    def init_producer_worker(self, name: str) -> None:
        self._mediator.add_worker_to_init_list(name, no_related=True)

    def init_consumer_worker(self, name: str) -> None:
        self._mediator.add_worker_to_init_list(name, no_related=False)
        self._mediator.add_worker_to_consume_list(name)

    def send_to(self, name: str, data: Union[Dict[str, Any], UniMessage], alone: bool = False) -> None:
        try:
            self._mediator.send_to(name, data, alone=alone)
        except PayloadError as e:
            self.echo.exit_with_error(f'invalid props in message: {e.validation_error}')

    def start_consuming(self) -> None:
        try:
            self._mediator.start_consuming()
        except KeyboardInterrupt:
            self.echo.log_warning('interrupted')
            exit(0)
