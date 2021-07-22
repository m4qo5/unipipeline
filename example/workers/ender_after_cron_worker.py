from example.messages.ender_after_cron_answer_message import EnderAfterCronAnswerMessage
from unipipeline import UniWorker, UniWorkerConsumerMessage

from example.messages.ender_after_cron_input_message import EnderAfterCronInputMessage


class EnderAfterCronWorker(UniWorker[EnderAfterCronInputMessage, EnderAfterCronAnswerMessage]):
    def handle_message(self, msg: UniWorkerConsumerMessage[EnderAfterCronInputMessage]) -> EnderAfterCronAnswerMessage:
        return EnderAfterCronAnswerMessage(
            value=f'after_cron => {msg.payload.value}'
        )
