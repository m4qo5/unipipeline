from unipipeline import UniWorker, UniCronMessage


class MySuperCronWorker(UniWorker[UniCronMessage, None]):
    def handle_message(self, message: UniCronMessage) -> None:
        print(f"!!! MySuperCronWorker.handle_message task_name={message.task_name}")
