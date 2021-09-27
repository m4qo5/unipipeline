from unipipeline.modules.uni_mediator import UniMediator


class UniUdpGateway:
    def __init__(self, port: int, worker_name: str, mediator: UniMediator) -> None:
        self._port = port
        self._mediator = mediator

    async def recived(self) -> None:
        # self._mediator.send_to(worker_name)
        pass

    async def start(self) -> None:
        pass
