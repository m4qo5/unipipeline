from unipipeline import UniWorker

from example.messages.input_message import InputMessage


class InputWorker(UniWorker):
    def handle_message(self, message: InputMessage) -> None:
        print("!!! InputWorker.handle_message. IT IS OK", message)
        raise NotImplementedError('some error in InputWorker.handle_message. IT IS OK')
