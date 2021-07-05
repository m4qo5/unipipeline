import json
from typing import Optional, Tuple, Any, Dict, List, NamedTuple, Set

from kafka import KafkaProducer, KafkaConsumer  # type: ignore
from kafka.consumer.fetcher import ConsumerRecord  # type: ignore
from pydantic import BaseModel

from unipipeline.modules.uni_broker import UniBroker, UniBrokerMessageManager, UniBrokerConsumer
from unipipeline.modules.uni_message_meta import UniMessageMeta


class UniKafkaBrokerMessageManager(UniBrokerMessageManager):
    def reject(self) -> None:
        pass

    def ack(self) -> None:
        pass


class UniKafkaBrokerConf(BaseModel):
    api_version: List[int]


class UniKafkaBrokerConsumer(NamedTuple):
    kfk_consumer: KafkaConsumer
    consumer: UniBrokerConsumer


class UniKafkaBroker(UniBroker[bytes]):
    def get_topic_approximate_messages_count(self, topic: str) -> int:
        return 0  # TODO

    def initialize(self, topics: Set[str]) -> None:
        pass

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.conf = UniKafkaBrokerConf(**self.definition.configure_dynamic(dict(
           api_version=None
        )))

        self._bootstrap_servers = self.get_boostrap_servers()

        self._producer: Optional[KafkaProducer] = None

        self._security_conf: Dict[str, Any] = self.get_security_conf()

        self._consumers: List[UniKafkaBrokerConsumer] = list()

        self._consuming_started = False

    def get_boostrap_servers(self) -> List[str]:
        raise NotImplementedError(f'method get_boostrap_server must be implemented for {type(self).__name__}')

    def get_security_conf(self) -> Dict[str, Any]:
        raise NotImplementedError(f'method get_security_conf must be implemented for {type(self).__name__}')

    def connect(self) -> None:
        if self._producer is not None:
            return

        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            api_version=self.conf.api_version,
            **self._security_conf,
        )

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
            self._producer = None
        for consumer in self._consumers:
            consumer.kfk_consumer.close()

    def _serialize_body(self, meta: UniMessageMeta) -> Tuple[bytes, bytes]:
        meta_dumps = self.definition.codec.dumps(meta.dict())
        return str(meta.id).encode('utf8'), bytes(meta_dumps, encoding='utf8')

    def _parse_body(self, msg: ConsumerRecord) -> UniMessageMeta:
        if msg.value.get('parent', None) is not None:
            return UniMessageMeta(**msg.value)
        return UniMessageMeta.create_new(data=msg.value)

    def add_topic_consumer(self, topic: str, consumer: UniBrokerConsumer) -> None:
        kfk_consumer = KafkaConsumer(
            topic,
            api_version=self.conf.api_version,
            bootstrap_servers=self._bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=consumer.group_id,
        )
        self._consumers.append(UniKafkaBrokerConsumer(kfk_consumer, consumer))

    def start_consuming(self) -> None:
        self.connect()
        if len(self._consumers) == 0:
            self.echo.log_warning('no consumers to start')
            return
        if self._consuming_started:
            raise OverflowError('consuming was started')
        self._consuming_started = True

        for cnsmr in self._consumers:  # TODO: make it asynchronously
            msg = next(cnsmr.kfk_consumer)
            meta = self._parse_body(msg)
            manager = UniKafkaBrokerMessageManager()
            cnsmr.consumer.message_handler(meta, manager)

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        self.connect()
        assert self._producer is not None
        for meta in meta_list:
            key, value = self._serialize_body(meta)
            self._producer.send(topic=topic, value=value, key=key)
        self._producer.flush()
