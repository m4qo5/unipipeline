from example.messages.ender_after_answer_message import EnderAfterAnswerMessage
from unipipeline import UniWorker, UniWorkerConsumerMessage

from example.messages.ender_after_input_message import EnderAfterInputMessage


class EnderAfterInputWorker(UniWorker[EnderAfterInputMessage, EnderAfterAnswerMessage]):
    def handle_message(self, msg: UniWorkerConsumerMessage[EnderAfterInputMessage]) -> EnderAfterAnswerMessage:
        return EnderAfterAnswerMessage(
            value=f'EnderAfterInputWorker answer on >>> {msg.payload.value}'
        )
