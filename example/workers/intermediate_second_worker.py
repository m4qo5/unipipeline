from unipipeline import UniWorker

from example.messages.inetermediate_message import InetermediateMessage


class IntermediateSecondWorker(UniWorker):
    def handle_message(self, message: InetermediateMessage) -> None:
        raise NotImplementedError('method handle_message must be specified for class "IntermediateSecondWorker"')
