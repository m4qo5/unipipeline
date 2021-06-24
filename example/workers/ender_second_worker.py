from unipipeline import UniWorker

from example.messages.ender_message import EnderMessage


class EnderSecondWorker(UniWorker):
    def handle_message(self, message: EnderMessage) -> None:
        raise NotImplementedError('method handle_message must be specified for class "EnderSecondWorker"')
