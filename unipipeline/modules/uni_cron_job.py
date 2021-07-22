from typing import NamedTuple, List, Tuple, Optional, Iterable, TYPE_CHECKING

from crontab import CronTab  # type: ignore

from unipipeline.message.uni_cron_message import UniCronMessage
from unipipeline.definitions.uni_cron_task_definition import UniCronTaskDefinition

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator


class UniCronJob(NamedTuple):
    id: int
    task: UniCronTaskDefinition
    crontab: CronTab
    mediator: 'UniMediator'
    message: UniCronMessage

    @staticmethod
    def mk_jobs_list(tasks: Iterable[UniCronTaskDefinition], mediator: 'UniMediator') -> List['UniCronJob']:
        res = list()
        for i, task in enumerate(tasks):
            res.append(UniCronJob(
                id=i,
                task=task,
                crontab=CronTab(task.when),
                mediator=mediator,
                message=UniCronMessage(task_name=task.name)
            ))
        return res

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
        self.mediator.send_to(self.task.worker.name, self.message, alone=self.task.alone)
