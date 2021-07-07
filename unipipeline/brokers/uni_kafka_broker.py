from typing import Optional, Tuple, Any, Dict, List, Set, Callable

from kafka import KafkaProducer, KafkaConsumer  # type: ignore
from kafka.consumer.fetcher import ConsumerRecord  # type: ignore

from unipipeline.modules.uni_broker import UniBroker, UniBrokerMessageManager, UniBrokerConsumer
from unipipeline.modules.uni_definition import UniDynamicDefinition
from unipipeline.modules.uni_message_meta import UniMessageMeta


class UniKafkaBrokerMessageManager(UniBrokerMessageManager):
    def __init__(self, commit: Callable[[], None]) -> None:
        self._commit = commit
        self._acknowledged = False

    def reject(self) -> None:
        pass

    def ack(self) -> None:
        if self._acknowledged:
            return
        self._acknowledged = True
        self._commit()


class UniKafkaBrokerConf(UniDynamicDefinition):
    api_version: Tuple[int, ...]
    retry_max_count: int = 100
    retry_delay_s: int = 3


class UniKafkaBroker(UniBroker[UniKafkaBrokerConf]):
    config_type = UniKafkaBrokerConf

    def get_boostrap_servers(self) -> List[str]:
        raise NotImplementedError(f'method get_boostrap_server must be implemented for {type(self).__name__}')

    def get_security_conf(self) -> Dict[str, Any]:
        raise NotImplementedError(f'method get_security_conf must be implemented for {type(self).__name__}')

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._bootstrap_servers = self.get_boostrap_servers()

        self._producer: Optional[KafkaProducer] = None

        self._security_conf: Dict[str, Any] = self.get_security_conf()

        self._consumers: List[UniBrokerConsumer] = list()
        self._kfk_active_consumers: List[KafkaConsumer] = list()

        self._consuming_started = False
        self._interrupted = False
        self._in_processing = False

    def stop_consuming(self) -> None:    # TODO
        self._end_consuming()

    def _end_consuming(self) -> None:
        if not self._consuming_started:
            return
        self._interrupted = True
        if not self._in_processing:
            for kfk_consumer in self._kfk_active_consumers:
                kfk_consumer.close()
            self._consuming_started = False
            self.echo.log_info('consumption stopped')

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        return 0  # TODO

    def initialize(self, topics: Set[str]) -> None:
        pass  # TODO

    def connect(self) -> None:
        if self._producer is not None:
            if self._producer._closed:
                self._producer.close()
                self._producer = None
            else:
                return

        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            api_version=self.config.api_version,
            value_serializer=lambda x: self.codec_serialize(x),
            retries=self.config.retry_max_count,
            acks=1,
            **self._security_conf,
        )

        if not self._producer.bootstrap_connected():
            raise ConnectionError()

        self.echo.log_info('connected')

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
            self._producer = None
        for kfk_consumer in self._kfk_active_consumers:
            kfk_consumer.close()

    def add_consumer(self, consumer: UniBrokerConsumer) -> None:
        self._consumers.append(consumer)

    def start_consuming(self) -> None:
        echo = self.echo.mk_child('consuming')
        if len(self._consumers) == 0:
            echo.log_warning('has no consumers to start consuming')
            return
        if self._consuming_started:
            echo.log_warning('consuming has already started. ignored')
            return
        self._consuming_started = True
        self._interrupted = False
        self._in_processing = False

        if len(self._consumers) != 1:
            raise OverflowError('invalid consumers number. this type of brokers not supports multiple consumers')

        consumer = self._consumers[0]
        kfk_consumer = KafkaConsumer(
            consumer.topic,
            api_version=self.config.api_version,
            bootstrap_servers=self._bootstrap_servers,
            enable_auto_commit=False,
            value_deserializer=lambda x: self.codec_parse(x),
            group_id=consumer.group_id,
        )

        self._kfk_active_consumers.append(kfk_consumer)

        def commit():
            kfk_consumer.commit()

        # TODO: retry
        for consumer_record in kfk_consumer:
            self._in_processing = True

            meta = consumer_record.value

            manager = UniKafkaBrokerMessageManager(commit)
            consumer.message_handler(meta, manager)

            self._in_processing = False
            if self._interrupted:
                self._end_consuming()
                break

        for kfk_consumer in self._kfk_active_consumers:
            kfk_consumer.close()

    def _get_producer(self) -> KafkaProducer:
        self.connect()
        assert self._producer is not None
        return self._producer

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        self.echo.log_debug(f'publishing the messages: {meta_list}')

        p = self._get_producer()

        for meta in meta_list:
            # TODO: retry
            p.send(
                topic=topic,
                value=meta,
                key=str(meta.id).encode('utf8')
            )
        p.flush()
