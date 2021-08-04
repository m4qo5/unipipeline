from ipaddress import IPv4Address

from pydantic import BaseModel

from unipipeline.message.uni_message import UniMessage


class UniGatewayBinMessageAddr(BaseModel):
    port: int
    ip: IPv4Address


class UniGatewayBinMessage(UniMessage):
    address_from: UniGatewayBinMessageAddr
    data: bytes
