from unipipeline import UniWorker

from example.messages.ender_after_cron_input_message import EnderAfterCronInputMessage


class EnderAfterCronWorker(UniWorker[EnderAfterCronInputMessage, None]):
    def handle_message(self, message: EnderAfterCronInputMessage) -> None:
        raise NotImplementedError('method handle_message must be specified for class "EnderAfterCronWorker"')
