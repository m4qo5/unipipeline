from typing import Any, Dict

from unipipeline import UniWorker

from example.messages.ender_message import EnderMessage


class EnderSecondWorker(UniWorker[EnderMessage, EnderMessage]):
    def handle_message(self, message: EnderMessage) -> Dict[str, Any]:
        print(f'!!! message {message}')
        return {
            "some_prop": "WORLD"
        }
