from unipipeline import UniWorker

from unipipeline.messages.uni_cron_message import UniCronMessage


class MySuperCronWorker(UniWorker):
    def handle_message(self, message: UniCronMessage) -> None:
        print(f"!!! MySuperCronWorker.handle_message task_name={message.task_name}")
