import functools
from typing import Optional, Any, Dict, List, Set, TYPE_CHECKING, Tuple

from kafka import KafkaProducer, KafkaConsumer  # type: ignore

from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.brokers.uni_broker_consumer import UniBrokerConsumer
from unipipeline.definitions.uni_broker_definition import UniBrokerDefinition
from unipipeline.definitions.uni_dynamic_definition import UniDynamicDefinition
from unipipeline.errors import UniMessageRejectError
from unipipeline.message_meta.uni_message_meta import UniMessageMeta

if TYPE_CHECKING:
    from unipipeline.modules.uni_mediator import UniMediator


class UniKafkaBrokerConfig(UniDynamicDefinition):
    api_version: Tuple[int, ...]
    retry_max_count: int = 100
    retry_delay_s: int = 3


class UniKafkaBroker(UniBroker[UniKafkaBrokerConfig]):
    config_type = UniKafkaBrokerConfig

    def get_boostrap_servers(self) -> List[str]:
        raise NotImplementedError(f'method get_boostrap_server must be implemented for {type(self).__name__}')

    def get_security_conf(self) -> Dict[str, Any]:
        raise NotImplementedError(f'method get_security_conf must be implemented for {type(self).__name__}')

    def __init__(self, mediator: 'UniMediator', definition: UniBrokerDefinition) -> None:
        super().__init__(mediator, definition)

        self._bootstrap_servers = self.get_boostrap_servers()

        self._producer: Optional[KafkaProducer] = None

        self._security_conf: Dict[str, Any] = self.get_security_conf()

        self._consumers: List[UniBrokerConsumer] = list()
        self._kfk_active_consumers: List[KafkaConsumer] = list()

        self._consuming_started = False
        self._interrupted = False
        self._in_processing = False

    def stop_consuming(self) -> None:    # TODO
        self._interrupted = True
        self._end_consuming()

    def _end_consuming(self, force: bool = False) -> None:
        if not self._consuming_started:
            return
        if self._in_processing and not force:
            return
        for kfk_consumer in self._kfk_active_consumers:
            kfk_consumer.close()
        self._kfk_active_consumers.clear()
        self._consuming_started = False
        self.echo.log_info('consumption stopped')

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        return 0  # TODO

    def initialize(self, topics: Set[str], answer_topic: Set[str]) -> None:
        pass  # TODO

    def connect(self) -> None:
        pass

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
            group_id=consumer.group_id,
            max_poll_records=1,
            max_poll_interval_ms=10 * 60_000,
            session_timeout_ms=10 * 60_000,
            request_timeout_ms=10 * 60_000 + 5,
            connections_max_idle_ms=10 * 60_000 + 15,
        )

        self._kfk_active_consumers.append(kfk_consumer)

        # TODO: retry
        exit_with_error = ''
        for consumer_record in kfk_consumer:
            self._in_processing = True
            echo.log_info(f'consuming message [{consumer_record.offset}]. started')

            get_meta = functools.partial(
                self.parse_message_body,
                content=consumer_record.value,
                compression=self.definition.compression,
                content_type=self.definition.content_type,
                unwrapped=consumer.unwrapped,
            )

            rejected = False
            try:
                consumer.message_handler(get_meta)
            except UniMessageRejectError as e:
                rejected = True
                echo.log_warning(f'consuming message [{consumer_record.offset}]. reject {type(e).__name__}. {e}')
            except Exception as e:  # noqa
                echo.log_error(f'consuming message [{consumer_record.offset}]. error {type(e).__name__}. {e}')
                raise

            self._in_processing = False
            if not rejected:
                try:
                    kfk_consumer.commit()
                except Exception as e:  # noqa
                    exit_with_error = f'consuming message [{consumer_record.offset}]. error {type(e).__name__}. {e}'
                    break
                else:
                    echo.log_info(f'consuming message [{consumer_record.offset}]. ok')
            if self._interrupted:
                break

        self._end_consuming(True)

        if exit_with_error:
            echo.exit_with_error(exit_with_error)

    def _connect_producer(self) -> None:
        if self._producer is not None:
            if self._producer._closed:
                self._producer.close()
                self._producer = None
            else:
                return

        # TODO: change default connection as producer yo abstract connection to kafka server
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            api_version=self.config.api_version,
            retries=self.config.retry_max_count,
            acks=1,
            **self._security_conf,
        )

        self.echo.log_info('connected')

    def _get_producer(self) -> KafkaProducer:
        self._connect_producer()
        assert self._producer is not None
        return self._producer

    def publish(self, topic: str, meta_list: List[UniMessageMeta], alone: bool = False) -> None:
        # TODO: alone
        # TODO: ttl
        # TODO: retry
        self.echo.log_debug(f'publishing the messages: {meta_list}')

        p = self._get_producer()

        for meta in meta_list:
            p.send(
                topic=topic,
                value=self.serialize_message_body(meta),
                key=str(meta.id).encode('utf8')
            )
        p.flush()
