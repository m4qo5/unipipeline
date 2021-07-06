import os.path
import importlib
from time import sleep
from typing import NamedTuple, Generic, Type, TypeVar, Any, Optional, Set

from unipipeline.modules.uni_echo import UniEcho
from unipipeline.utils.template import template

T = TypeVar('T')

uni_broker_template = '''
from typing import Set, List

from unipipeline import UniBroker, UniMessageMeta, UniBrokerConsumer


class {{name}}(UniBroker):

    def stop_consuming(self) -> None:
        raise NotImplementedError('method stop_consuming must be specified for class "{{name}}"')

    def connect(self) -> None:
        raise NotImplementedError('method connect must be specified for class "{{name}}"')

    def close(self) -> None:
        raise NotImplementedError('method close must be specified for class "{{name}}"')

    def add_topic_consumer(self, topic: str, consumer: UniBrokerConsumer) -> None:
        raise NotImplementedError('method add_topic_consumer must be specified for class "{{name}}"')

    def start_consuming(self) -> None:
        raise NotImplementedError('method start_consuming must be specified for class "{{name}}"')

    def publish(self, topic: str, meta_list: List[UniMessageMeta]) -> None:
        raise NotImplementedError('method publish must be specified for class "{{name}}"')

    def get_topic_approximate_messages_count(self, topic: str) -> int:
        raise NotImplementedError('method get_topic_approximate_messages_count must be specified for class "{{name}}"')

    def initialize(self, topics: Set[str]) -> None:
        raise NotImplementedError('method initialize must be specified for class "{{name}}"')

'''

uni_message_template = '''from unipipeline import UniMessage


class {{name}}(UniMessage):
    pass

'''


uni_worker_template = '''from unipipeline import UniWorker

from {{data.input_message.type.module}} import {{data.input_message.type.class_name}}


class {{name}}(UniWorker):
    def handle_message(self, message: {{data.input_message.type.class_name}}) -> None:
        raise NotImplementedError('method handle_message must be specified for class "{{name}}"')

'''


uni_waiting_template = '''from unipipeline import UniWaiting


class {{name}}(UniWaiting):
    def try_to_connect(self) -> None:
        raise NotImplementedError('method try_to_connect must be specified for class "{{name}}"')

'''

tpl_map = {
    "UniBroker": uni_broker_template,
    "UniMessage": uni_message_template,
    "UniWorker": uni_worker_template,
    "UniWaiting": uni_waiting_template,
}


CWD = str(os.getcwdb().decode('utf-8'))


class UniModuleDefinition(NamedTuple, Generic[T]):
    module: str
    class_name: str

    @staticmethod
    def parse(type_def: str) -> 'UniModuleDefinition':
        assert isinstance(type_def, str), f"type_def must be str. {type(type_def)} given"
        spec = type_def.split(":")
        assert len(spec) == 2, f'must have 2 segments. {len(spec)} was given from "{type_def}"'
        return UniModuleDefinition(
            module=spec[0],
            class_name=spec[1],
        )

    def import_class(self, class_type: Type[T], echo: UniEcho, auto_create: bool = False, create_template_params: Any = None) -> Type[T]:
        try:
            mdl = importlib.import_module(self.module)
        except ModuleNotFoundError:
            if not auto_create:
                raise
            mdl = None  # type: ignore
            echo = echo.mk_child(f'module[{self.module}::{self.class_name}]')
            hierarchy = self.module.split('.')
            path = os.path.abspath(f'{os.path.join("./", *hierarchy)}.py')
            path_dir = os.path.dirname(path)
            os.makedirs(path_dir, exist_ok=True)

            path_inits: Set[str] = {os.path.join(path_dir, "__init__.py"), }

            if path_dir.startswith(CWD):
                current_dir: Optional[str] = None
                for pi_dir in path_dir[len(CWD):].strip('/').split('/'):
                    if current_dir is not None:
                        pi_dir = os.path.join(current_dir, pi_dir)
                    current_dir = pi_dir
                    path_inits.add(os.path.join(CWD, pi_dir, "__init__.py"))

            for pi in path_inits:
                if not os.path.isfile(pi):
                    with open(pi, "wt+") as fi:
                        fi.write("")
                    echo.log_debug(f'file {pi} was created')

            with open(path, 'wt') as fm:
                fm.writelines(template(tpl_map[class_type.__name__], data=create_template_params, name=self.class_name))
                echo.log_info(f'file {path} was created')

            success = False
            for i in range(10):  # because fs has cache time
                try:
                    importlib.invalidate_caches()
                    mdl = importlib.import_module(self.module)
                    success = True
                    break
                except ModuleNotFoundError:
                    echo.log_debug(f'still not found. try to waiting for {i}s before invalidating the cache')
                    sleep(i)
            if not success:
                echo.log_error(f'could not be loaded')
                exit(1)
        assert mdl is not None
        tp = getattr(mdl, self.class_name)
        if not issubclass(tp, class_type):
            ValueError(f'class {self.class_name} is not subclass of {class_type.__name__}')
        return tp
