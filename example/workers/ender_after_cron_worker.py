from example.messages.ender_after_cron_answer_message import EnderAfterCronAnswerMessage
from example.messages.ender_after_cron_input_message import EnderAfterCronInputMessage
from unipipeline import UniWorker, UniWorkerConsumerMessage


class EnderAfterCronWorker(UniWorker[EnderAfterCronInputMessage, EnderAfterCronAnswerMessage]):
    async def handle_message(self, msg: UniWorkerConsumerMessage[EnderAfterCronInputMessage]) -> EnderAfterCronAnswerMessage:
        return EnderAfterCronAnswerMessage(
            value=f'after_cron => {msg.payload.value}'
        )
