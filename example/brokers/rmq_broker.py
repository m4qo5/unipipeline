from unipipeline import UniAmqpPikaBroker


class RmqBroker(UniAmqpPikaBroker):

    @classmethod
    def get_connection_uri(cls) -> str:
        return 'amqp://admin:admin@localhost:25672'
