import asyncio
from typing import Dict, TypeVar, Any, Union, Optional, Type

from unipipeline.answer.uni_answer_message import UniAnswerMessage
from unipipeline.brokers.uni_broker import UniBroker
from unipipeline.config.uni_config import UniConfig
from unipipeline.definitions.uni_worker_definition import UniWorkerDefinition
from unipipeline.errors.uni_payload_error import UniPayloadSerializationError
from unipipeline.errors.uni_work_flow_error import UniWorkFlowError
from unipipeline.message.uni_message import UniMessage
from unipipeline.message_meta.uni_message_meta import UniMessageMeta, UniMessageMetaErrTopic, UniAnswerParams
from unipipeline.modules.uni_session import UniSession
from unipipeline.utils.uni_echo import UniEcho
from unipipeline.utils.uni_util import UniUtil
from unipipeline.worker.uni_worker import UniWorker
from unipipeline.worker.uni_worker_consumer import UniWorkerConsumer

TWorker = TypeVar('TWorker', bound=UniWorker[Any, Any])


class UniMediator:
    def __init__(self, util: UniUtil, echo: UniEcho, config: UniConfig, session: UniSession, loop: asyncio.AbstractEventLoop) -> None:
        self._config = config
        self._echo = echo
        self._util = util
        self._loop = loop
        self._session = session

        self._worker_instance_indexes: Dict[str, UniWorkerConsumer[Any, Any]] = dict()
        self._broker_instance_indexes: Dict[str, UniBroker[Any]] = dict()

    @property
    def echo(self) -> UniEcho:
        return self._echo

    def set_echo_level(self, level: int) -> None:
        self.echo.level = level

    def get_broker(self, name: str, singleton: bool = True) -> UniBroker[Any]:
        if not singleton:
            bd = self.config.brokers[name]
            broker_type = self._config.get_broker_type(name)
            br = broker_type(mediator=self, definition=bd, loop=self._loop)
            return br
        if name not in self._broker_instance_indexes:
            self._broker_instance_indexes[name] = self.get_broker(name, singleton=False)
        return self._broker_instance_indexes[name]

    def get_worker_consumer(self, worker: Union[Type['UniWorker[Any, Any]'], str], singleton: bool = True) -> UniWorkerConsumer[Any, Any]:
        wd = self._config.get_worker_definition(worker)
        if wd.marked_as_external:
            raise OverflowError(f'worker "{worker}" is external. you could not get it')
        if not singleton or wd.name not in self._worker_instance_indexes:
            worker_type = self._config.get_worker_type(wd.name)
            wc = UniWorkerConsumer(wd, self, worker_type)
        else:
            return self._worker_instance_indexes[wd.name]
        self._worker_instance_indexes[wd.name] = wc
        return wc

    async def move_to_error_topic(self, wd: UniWorkerDefinition, meta: UniMessageMeta, err_topic: UniMessageMetaErrTopic, err: Exception) -> None:
        self._echo.log_error(str(err))
        meta = meta.create_error_child(err_topic, err)
        br = self.get_broker(wd.broker.name)
        error_topic = wd.error_topic
        if error_topic == UniMessageMetaErrTopic.MESSAGE_PAYLOAD_ERR.value:
            error_topic = wd.error_payload_topic
        await br.publish(error_topic, [meta])

    async def answer_to(self, worker_name: str, req_meta: UniMessageMeta, payload: Optional[Union[Dict[str, Any], UniMessageMeta, UniMessage]], unwrapped: bool) -> None:
        wd = self._config.workers[worker_name]
        if not wd.need_answer:
            if payload is not None:
                raise UniWorkFlowError(f'output message must be None because worker {wd.name} has no possibility to send output messages')
            return

        if payload is None:
            raise UniPayloadSerializationError('output message must be not empty')

        answ_meta: UniMessageMeta
        if isinstance(payload, UniMessageMeta):
            answ_meta = payload
        else:
            assert wd.answer_message is not None
            answ_message_type = self.config.get_message_type(wd.answer_message.name)
            payload_msg: UniMessage
            if isinstance(payload, answ_message_type):
                payload_msg = payload
            elif isinstance(payload, dict):
                try:
                    payload_msg = answ_message_type(**payload)  # type: ignore
                except Exception as e:
                    raise UniPayloadSerializationError(str(e))
            else:
                raise UniPayloadSerializationError(f'output message has invalid type. {type(payload).__name__} was given')

            answ_meta = req_meta.create_child(payload_msg.dict(), unwrapped=unwrapped)

        b = self.get_broker(wd.broker.name)

        assert req_meta.answer_params is not None
        await b.publish_answer(req_meta.answer_params, answ_meta)
        self.echo.log_info(f'worker {worker_name} answers to {req_meta.answer_params.topic}->{req_meta.answer_params.id} :: {answ_meta}')

    async def send_to(
        self,
        worker_name: str,
        payload: Union[Dict[str, Any], UniMessage],
        parent_meta: Optional[UniMessageMeta] = None,
        answer_params: Optional[UniAnswerParams] = None,
        alone: bool = False
    ) -> Optional[UniAnswerMessage[UniMessage]]:
        if not self._session.is_worker_initialized(worker_name):
            raise OverflowError(f'worker {worker_name} was not initialized')

        wd = self._config.workers[worker_name]

        message_type = self.config.get_message_type(wd.input_message.name)
        try:
            if isinstance(payload, message_type):
                payload_data = payload.dict()
            elif isinstance(payload, dict):
                payload_data = message_type(**payload).dict()  # type: ignore
            else:
                raise TypeError(f'data has invalid type.{type(payload).__name__} was given')
        except Exception as e:
            raise UniPayloadSerializationError(str(e))

        br = self.get_broker(wd.broker.name)

        if alone:
            size = await br.get_topic_approximate_messages_count(wd.topic)
            if size != 0:
                self.echo.log_info(f'sending to worker "{wd.name}" was skipped, because topic {wd.topic} has messages: {size}>0')
                return None

        if parent_meta is not None:
            meta = parent_meta.create_child(payload_data, unwrapped=wd.input_unwrapped, answer_params=answer_params)
        else:
            meta = UniMessageMeta.create_new(payload_data, unwrapped=wd.input_unwrapped, answer_params=answer_params)

        meta_list = [meta]
        await br.publish(wd.topic, meta_list)  # TODO: make it list by default
        self.echo.log_info(f"sent message to topic '{wd.topic}':: {meta_list}")

        if meta.need_answer and wd.need_answer:
            assert wd.answer_message is not None
            assert answer_params is not None
            answ_meta = await br.get_answer(answer_params, max_delay_s=wd.answer_avg_delay_s * 3, unwrapped=wd.answer_unwrapped)
            answ_message_type = self.config.get_message_type(wd.answer_message.name)
            return UniAnswerMessage(answ_meta, answ_message_type)
        return None

    def decompress_message_body(self, compression: Optional[str], data: Union[str, bytes]) -> bytes:
        data_bytes: bytes
        if isinstance(data, str):
            data_bytes = bytes(data, encoding='utf-8')
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            raise TypeError('invalid type')
        if compression is not None:
            decompressor = self.config.get_decompressor(compression)
            return decompressor(data_bytes)
        return data_bytes

    def compress_message_body(self, compression: Optional[str], data: Union[str, bytes]) -> bytes:
        data_bytes: bytes
        if isinstance(data, str):
            data_bytes = bytes(data, encoding='utf-8')
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            raise TypeError('invalid type')
        if compression is not None:
            compressor = self.config.get_compressor(compression)
            return compressor(data_bytes)
        return data_bytes

    def parse_content_type(self, content_type: str, data: Union[bytes, str]) -> Dict[str, Any]:
        data_str: str
        if isinstance(data, str):
            data_str = data
        elif isinstance(data, bytes):
            data_str = data.decode('utf-8')
        else:
            raise TypeError('invalid type')

        parser = self.config.get_content_type_parser(content_type)
        return parser(data_str)

    def serialize_content_type(self, content_type: str, data: Dict[str, Any]) -> bytes:
        if not isinstance(data, dict):
            raise TypeError(f'invalid type of payload. must be dict, {type(data).__name__} was given')

        serializer = self.config.get_content_type_serializer(content_type)
        res = serializer(data)
        if isinstance(res, str):
            return res.encode('utf-8')
        return res

    @property
    def config(self) -> UniConfig:
        return self._config

    async def wait_for_broker_connection(self, name: str) -> UniBroker[Any]:
        br = self.get_broker(name)
        for try_count in range(br.definition.retry_max_count):
            try:
                await br.connect()
                self.echo.log_info(f'wait_for_broker_connection :: broker {br.definition.name} connected')
                return br
            except ConnectionError as e:
                self.echo.log_info(f'wait_for_broker_connection :: broker {br.definition.name} retry to connect [{try_count}/{br.definition.retry_max_count}] : {e}')
                await asyncio.sleep(br.definition.retry_delay_s)
                continue
        raise ConnectionError(f'unavailable connection to {br.definition.name}')
