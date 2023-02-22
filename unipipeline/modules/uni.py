from typing import Dict, Any, Union, Optional, List

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.config.uni_config import UniConfig
from unipipeline.errors import UniConfigError, UniPayloadSerializationError
from unipipeline.message.uni_message import UniMessage
from unipipeline.modules.uni_mediator import UniMediator
from unipipeline.utils.uni_echo import UniEcho
from unipipeline.waiting.uni_wating import UniWaiting
from unipipeline.worker.uni_msg_params import default_sending_params, UniSendingParams
from unipipeline.worker.uni_worker import UniWorker


class Uni:
    def __init__(self, config: Union[UniConfig, str]) -> None:
        if isinstance(config, str):
            config = UniConfig.from_file(config)

        if not isinstance(config, UniConfig):
            raise ValueError(f'invalid config type. {type(config).__name__} was given')

        self._mediator = UniMediator(config)
        config._init(
            util=self._mediator._util,
            echo=self._mediator.echo,
        )

    @property
    def echo(self) -> UniEcho:
        return self._mediator.echo

    @property
    def config(self) -> UniConfig:
        return self._mediator.config

    def set_echo_level(self, level: int) -> None:
        self._mediator.set_echo_level(level)

    def check(self) -> None:
        try:
            for waiting_def in self._mediator.config.waitings.values():
                assert waiting_def.import_template.import_value is not None

            for broker_def in self._mediator.config.brokers.values():
                assert broker_def.import_template.import_value is not None

            for message_def in self._mediator.config.messages.values():
                assert message_def.import_template.import_value is not None

            for worker_def in self._mediator.config.workers.values():
                if worker_def.marked_as_external:
                    continue
                assert worker_def.import_template is None or worker_def.import_template.import_value is not None

        except (UniConfigError, AssertionError) as e:
            self.echo.exit_with_error(str(e))

    def start_cron(self) -> None:
        try:
            self._mediator.start_cron()
        except KeyboardInterrupt:
            self.echo.log_warning('interrupted')
            exit(0)

    def initialize_cron_producer_workers(self) -> None:
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

    def send_to(self, name: str, data: Union[Dict[str, Any], UniMessage, List[Dict[str, Any]], List[UniMessage]], params: UniSendingParams = default_sending_params) -> None:
        try:
            self._mediator.send_to(name, data, params=params)
        except UniPayloadSerializationError as e:
            self.echo.exit_with_error(f'invalid props in message: {e}')

    def exit(self) -> None:
        self.echo.log_info('exit!')
        self._mediator.exit()

    def start_consuming(self) -> None:
        try:
            self._mediator.start_consuming()
        except KeyboardInterrupt:
            self.echo.log_warning('interrupted')
            exit(0)
