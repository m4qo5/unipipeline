from unipipeline import UniWorker

from example.messages.inetermediate_message import InetermediateMessage


class IntermediateFirstWorker(UniWorker):
    def handle_message(self, message: InetermediateMessage) -> None:
        raise NotImplementedError('method handle_message must be specified for class "IntermediateFirstWorker"')
