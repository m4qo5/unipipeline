import asyncio
from asyncio import BaseTransport
from typing import Optional


async def echo(data: bytes) -> bytes:
    data = b'>' + data
    await asyncio.sleep(1)
    return data


class SProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.data_queue = asyncio.Queue(loop=asyncio.get_event_loop())  # type: ignore
        super().__init__()
        self.transport: Optional[BaseTransport] = None

    def connection_made(self, transport: BaseTransport) -> None:
        self.transport = transport

    def error_received(self, exc: Optional[Exception]) -> None:
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        pass

    # def datagram_received(self, data: Tuple[str, int]) -> None:
    #     asyncio.ensure_future(self.handler(data), loop=asyncio.get_event_loop())

    async def respond(self) -> None:
        assert self.transport is not None
        while True:
            self.transport.sendto(*await self.data_queue.get())  # type: ignore

    async def handler(self, data: bytes, addr: int) -> None:
        assert self.transport is not None
        data = await echo(data)
        # self.data_queue.put((data, caller))
        message = data.decode()
        print('Received %r from %s' % (message, addr))
        print('Send %r to %s' % (message, addr))
        self.transport.sendto(data, addr)  # type: ignore


print("Starting UDP server")

loop = asyncio.get_event_loop()

transport, protocol = loop.run_until_complete(loop.create_datagram_endpoint(
    lambda: SProtocol(),
    local_addr=('127.0.0.1', 9999),
))
print(transport, protocol)

try:
    loop.run_forever()
finally:
    transport.close()
