import asyncio
from typing import Dict, Any, Union, Optional, Iterable
from uuid import uuid4

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.brokers.uni_broker import UniBrokerConsumer
from unipipeline.config.uni_config import UniConfig, UniConfigError
from unipipeline.errors.uni_payload_error import UniPayloadSerializationError
from unipipeline.message.uni_message import UniMessage
from unipipeline.modules.uni_cron_job import UniCronJob
from unipipeline.modules.uni_mediator import UniMediator
from unipipeline.modules.uni_session import UniSession
from unipipeline.utils.uni_echo import UniEcho
from unipipeline.utils.uni_util import UniUtil
from unipipeline.waiting.uni_wating import UniWaiting
from unipipeline.worker.uni_worker import UniWorker


def camel_case(snake_cased: str) -> str:
    return ''.join(word.title() for word in snake_cased.split('_'))


class Uni:
    def __init__(self, config: Union[UniConfig, str], echo_level: Optional[Union[str, int]] = None, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._util = UniUtil()
        self._util.template.set_filter('camel', camel_case)

        self._echo = UniEcho('UNI', level=echo_level, colors=self._util.color)

        if isinstance(config, str):
            config = UniConfig(self._util, self._echo, config)
        if not isinstance(config, UniConfig):
            raise ValueError(f'invalid config type. {type(config).__name__} was given')
        self._session = UniSession(config, self._echo, self._util)
        self._mediator = UniMediator(self._util, self._echo, config, self._session, loop)

    @property
    def echo(self) -> UniEcho:
        return self._mediator.echo

    @property
    def util(self) -> UniUtil:
        return self._util

    @property
    def config(self) -> UniConfig:
        return self._mediator.config

    def set_echo_level(self, level: int) -> None:
        self._mediator.set_echo_level(level)

    def scaffold(self) -> None:
        try:
            for waiting_def in self._mediator.config.waitings.values():
                waiting_def.type.import_class(UniWaiting, self.echo, auto_create=True, create_template_params=waiting_def, util=self._util)

            for broker_def in self._mediator.config.brokers.values():
                broker_def.type.import_class(UniBroker, self.echo, auto_create=True, create_template_params=broker_def, util=self._util)

            for message_def in self._mediator.config.messages.values():
                message_def.type.import_class(UniMessage, self.echo, auto_create=True, create_template_params=message_def, util=self._util)

            for worker_def in self._mediator.config.workers.values():
                if worker_def.marked_as_external:
                    continue
                assert worker_def.type is not None
                worker_def.type.import_class(UniWorker, self.echo, auto_create=True, create_template_params=worker_def, util=self._util)

        except UniConfigError as e:
            self.echo.exit_with_error(str(e))

    def check(self) -> None:
        try:
            for waiting_def in self._mediator.config.waitings.values():
                waiting_def.type.import_class(UniWaiting, self.echo, util=self._util)

            for broker_def in self._mediator.config.brokers.values():
                broker_def.type.import_class(UniBroker, self.echo, util=self._util)

            for message_def in self._mediator.config.messages.values():
                message_def.type.import_class(UniMessage, self.echo, util=self._util)

            for worker_def in self._mediator.config.workers.values():
                if worker_def.marked_as_external:
                    continue
                assert worker_def.type is not None
                worker_def.type.import_class(UniWorker, self.echo, util=self._util)

        except UniConfigError as e:
            self.echo.exit_with_error(str(e))

    def initialize(self, everything: bool = False) -> None:
        self._loop.run_until_complete(self._session.initialize(self._mediator, everything=everything, create=True))

    async def _send_to(self, name: str, data: Union[Dict[str, Any], UniMessage], alone: bool) -> None:
        try:
            await self._mediator.send_to(name, data, alone=alone)
        except UniPayloadSerializationError as e:
            self.echo.exit_with_error(f'invalid props in message: {e}')

    def send_to(self, name: str, data: Union[Dict[str, Any], UniMessage], alone: bool = False) -> None:
        self._session.init_producer_worker(name)
        self.initialize()
        return self._loop.run_until_complete(self._send_to(name, data, alone))

    async def _start_cron(self) -> None:
        cron_jobs = UniCronJob.mk_jobs_list(self.config.cron_tasks.values(), self._mediator)
        self.echo.log_debug(f'cron jobs defined: {", ".join(cj.task.name for cj in cron_jobs)}')
        while True:
            delay, jobs = UniCronJob.search_next_tasks(cron_jobs)
            if delay is None:
                return
            self.echo.log_debug(f"sleep {delay} seconds before running the tasks: {[cj.task.name for cj in jobs]}")
            if delay > 0:
                await asyncio.sleep(delay)
            self.echo.log_info(f"run the tasks: {[cj.task.name for cj in jobs]}")

            sending_f = []
            for cj in jobs:
                sending_f.append(cj.send())
            await asyncio.gather(*sending_f)
            await asyncio.sleep(1.1)  # delay for correct next iteration

    def start_cron(self) -> None:
        self._session.init_cron_producers()
        self.initialize()
        try:
            self._loop.run_until_complete(self._start_cron())
        except KeyboardInterrupt:
            self.echo.log_warning('interrupted')
            exit(0)

    def start_consuming(self, worker_group: Optional[str] = None, workers: Optional[Iterable[str]] = None, ) -> None:
        if worker_group is None and not workers:
            return
        if workers:
            for wn in workers:
                self._session.init_consumer_worker(wn)
        if worker_group:
            self._session.init_consumer_worker_group(worker_group)
        self.initialize()
        try:
            # TODO: interrupted

            brokers = set()
            bf = list()
            for wn in self._session.get_consumer_worker_names():
                wd = self._mediator.config.workers[wn]
                wc = self._mediator.get_worker_consumer(wn)
                bn = wd.broker.name

                b = self._mediator.get_broker(bn)

                self.echo.log_info(f"worker {wn} start consuming")
                b.add_consumer(UniBrokerConsumer(
                    topic=wd.topic,
                    id=f'{wn}__{uuid4()}',
                    group_id=wn,
                    unwrapped=wd.input_unwrapped,
                    message_handler=wc.process_message
                ))

                self.echo.log_info(f'consumer {wn} initialized')
                if bn in brokers:
                    continue
                brokers.add(bn)

                self._session.add_broker_with_active_consumption(b)
                self.echo.log_info(f'broker {bn} consuming start')
                bf.append(b.start_consuming())

            self._loop.run_until_complete(asyncio.gather(*bf))

            self._loop.run_forever()
        except KeyboardInterrupt:
            self.echo.log_warning('interrupted')
            self._loop.run_until_complete(self._session.interrupt())
        except Exception as e:
            self.echo.log_error(str(e))
            self._loop.run_until_complete(self._session.interrupt())
        else:
            self._loop.run_until_complete(self._session.interrupt())
        finally:
            try:
                self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            finally:
                self._loop.close()
        self.echo.log_warning('done')
