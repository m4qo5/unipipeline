import json
from typing import Optional, Tuple, Callable, Any, Dict

from kafka import KafkaProducer, KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from unipipeline.modules.uni_broker import UniBroker, UniBrokerMessageManager
from unipipeline.modules.uni_broker_definition import UniBrokerDefinition
from unipipeline.modules.uni_message_meta import UniMessageMeta


class KafkaUniBrokerMessageManager(UniBrokerMessageManager):
    def reject(self) -> None:
        pass

    def ack(self) -> None:
        pass


class UniKafkaBroker(UniBroker):
    def __init__(self, definition: UniBrokerDefinition[bytes]) -> None:
        super().__init__(definition)

        self._bootstrap_servers = self.definition.kafka_definition.bootstrap_servers

        self._consumer: Optional[KafkaConsumer] = None
        self._producer: Optional[KafkaProducer] = None

        self._security_conf = self._get_security_conf()

    def _get_security_conf(self) -> Dict[str, Any]:
        raise NotImplementedError(f'method get_security_conf must be implemented for {type(self).__name__}')

    def connect(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            api_version=self.definition.kafka_definition.api_version,
            **self._security_conf,
        )

    def close(self) -> None:
        if self._consumer is not None:
            self.stop_consuming()
        if self._producer is not None:
            self.stop_producing()

    def stop_producing(self) -> None:
        assert self._producer is not None
        self._producer.close()
        self._producer = None

    def stop_consuming(self) -> None:
        assert self._consumer is not None
        self._consumer.close()
        self._consumer = None

    def serialize_body(self, meta: UniMessageMeta) -> Tuple[bytes, bytes]:
        meta_dumps = self.definition.message_codec.dumps(meta.dict())
        return str(meta.id).encode('utf8'), bytes(meta_dumps, encoding='utf8')

    def parse_body(self, msg: ConsumerRecord) -> UniMessageMeta:
        if msg.value.get('parent', None) is not None:
            return UniMessageMeta(**msg.value)
        return UniMessageMeta.create_new(data=msg.value)

    def consume(
        self,
        topic: str,
        processor: Callable[[UniMessageMeta, UniBrokerMessageManager], None],
        consumer_tag: str,
        worker_name: str,
        prefetch: int = 1
    ) -> None:

        self._consumer = KafkaConsumer(
            topic,
            api_version=self.definition.kafka_definition.api_version,
            bootstrap_servers=self._bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=worker_name,
        )

        for msg in self._consumer:
            meta = self.parse_body(msg)
            manager = KafkaUniBrokerMessageManager()
            processor(meta, manager)

    def publish(self, topic: str, meta: UniMessageMeta) -> None:
        self.connect()
        key, value = self.serialize_body(meta)
        assert self._producer is not None
        self._producer.send(topic=topic, value=value, key=key)
        self._producer.flush()
