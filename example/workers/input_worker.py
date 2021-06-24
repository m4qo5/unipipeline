from unipipeline import UniWorker

from example.messages.input_message import InputMessage


class InputWorker(UniWorker):
    def handle_message(self, message: InputMessage) -> None:
        print("!!!")
        raise NotImplementedError('method handle_message must be specified for class "InputWorker"')
