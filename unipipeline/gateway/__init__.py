import asyncio


async def echo(data):
    #external lib emulator
    data = b'>'+data
    await asyncio.sleep(1)
    return data


class SProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.data_queue = asyncio.Queue(loop=asyncio.get_event_loop())
        super().__init__()
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def error_received(self, exc):
        pass

    def connection_lost(self, exc):
        pass

    def datagram_received(self, data, addr):
        asyncio.ensure_future(self.handler(data, addr), loop=asyncio.get_event_loop())

    async def respond(self):
        while True:
            resp, caller = await self.data_queue.get()
            self.transport.sendto(resp, caller)

    async def handler(self, data, addr):
        data = await echo(data)
        self.data_queue.put((data, caller))
        message = data.decode()
        print('Received %r from %s' % (message, addr))
        print('Send %r to %s' % (message, addr))
        self.transport.sendto(data, addr)


print("Starting UDP server")

loop = asyncio.get_event_loop()

transport, protocol = loop.run_until_complete(loop.create_datagram_endpoint(
    lambda: SProtocol(),  # type: ignore
    local_addr=('127.0.0.1', 9999),
))
print(transport, protocol)

try:
    loop.run_forever()
finally:
    transport.close()
