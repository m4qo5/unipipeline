from unipipeline import UniWorker

from unipipeline.messages.uni_cron_message import UniCronMessage


class MySuperCronWorker(UniWorker[UniCronMessage, None]):
    def handle_message(self, message: UniCronMessage) -> None:
        raise NotImplementedError('method handle_message must be specified for class "MySuperCronWorker"')
