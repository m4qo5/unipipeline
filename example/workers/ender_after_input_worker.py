from unipipeline import UniWorker

from example.messages.ender_after_input_message import EnderAfterInputMessage


class EnderAfterInputWorker(UniWorker[EnderAfterInputMessage, None]):
    def handle_message(self, message: EnderAfterInputMessage) -> None:
        raise NotImplementedError('method handle_message must be specified for class "EnderAfterInputWorker"')
