from unipipeline.brokers.uni_amqp_broker import UniAmqpBroker


class RmqBroker(UniAmqpBroker):

    @classmethod
    def get_connection_uri(cls) -> str:
        return 'amqp://admin:admin@localhost:25672'
