from typing import NamedTuple, List, Tuple, Optional

from crontab import CronTab  # type: ignore

from unipipeline.messages.uni_cron_message import UniCronMessage
from unipipeline.modules.uni_cron_task_definition import UniCronTaskDefinition
from unipipeline.modules.uni_worker import UniWorker
from unipipeline.modules.uni_message import UniMessage


class UniCronJob(NamedTuple):
    id: int
    name: str
    crontab: CronTab
    worker: UniWorker[UniMessage]
    message: UniCronMessage

    @staticmethod
    def new(id: int, task_def: UniCronTaskDefinition, worker: UniWorker[UniMessage]) -> 'UniCronJob':
        return UniCronJob(
            id=id,
            name=task_def.name,
            crontab=CronTab(task_def.when),
            worker=worker,
            message=UniCronMessage(
                task_name=task_def.name
            )
        )

    @staticmethod
    def search_next_tasks(all_tasks: List['UniCronJob']) -> Tuple[Optional[int], List['UniCronJob']]:
        min_delay: Optional[int] = None
        notification_list: List[UniCronJob] = []
        for cj in all_tasks:
            sec = int(cj.crontab.next(default_utc=False))
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
