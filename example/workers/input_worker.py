from datetime import datetime

from typing import Optional

from example.messages.ender_after_answer_message import EnderAfterAnswerMessage
from example.messages.ender_after_input_message import EnderAfterInputMessage
from example.messages.input_message import InputMessage
from example.messages.some_external_message import SomeExternalMessage
from unipipeline.answer.uni_answer_message import UniAnswerMessage
from unipipeline.worker.uni_msg_params import UniSendingParams
from unipipeline.worker.uni_worker import UniWorker
from unipipeline.worker.uni_worker_consumer_message import UniWorkerConsumerMessage


class InputWorker(UniWorker[InputMessage, None]):
    def handle_message(self, msg: UniWorkerConsumerMessage[InputMessage]) -> None:
        # answ = '!!!'  # * 100000

        self.manager.send_to('ender_after_input_worker', EnderAfterInputMessage(
            value=f'from input_worker {datetime.now()}'
        ))

        self.manager.send_to('ender_after_input_worker', EnderAfterInputMessage(
            value=f'from input_worker {datetime.now()}'
        ))

        answ_msg: Optional[UniAnswerMessage[EnderAfterAnswerMessage]] = self.manager.get_answer_from('ender_after_input_worker', EnderAfterInputMessage(value=f'from input_worker {datetime.now()}'))
        assert answ_msg is not None
        answ = answ_msg.payload.value

        self.manager.send_to('some_external_worker', SomeExternalMessage(
            value=f'answ: {answ[:30]} ==> from input_worker {datetime.now()}'
        ), UniSendingParams(ttl_s=15))
