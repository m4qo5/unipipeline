import os.path
from importlib import import_module, invalidate_caches
from time import sleep
from typing import NamedTuple, Generic, Type, TypeVar, Any

from unipipeline.utils.template import template

T = TypeVar('T')

uni_broker_template = '''
from typing import Callable

from unipipeline import UniBroker, UniMessageMeta, UniBrokerMessageManager


class {{name}}(UniBroker):
    def connect(self) -> None:
        raise NotImplementedError('method connect must be specified for class "{{name}}"')

    def close(self) -> None:
        raise NotImplementedError('method close must be specified for class "{{name}}"')

    def consume(self, topic: str, processor: Callable[[UniMessageMeta, UniBrokerMessageManager], None], consumer_tag: str, worker_name: str, prefetch: int = 1) -> None:
        raise NotImplementedError('method consume must be specified for class "{{name}}"')

    def publish(self, topic: str, meta: UniMessageMeta) -> None:
        raise NotImplementedError('method publish must be specified for class "{{name}}"')

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


class UniModuleDefinition(NamedTuple, Generic[T]):
    module: str
    class_name: str

    def import_class(self, class_type: Type[T], auto_create: bool = False, create_template_params: Any = None) -> Type[T]:
        try:
            mdl = import_module(self.module)
        except ModuleNotFoundError:
            if auto_create:
                hierarchy = self.module.split('.')
                path = f'{os.path.join("./", *hierarchy)}.py'
                path_dir = os.path.dirname(path)
                path_init = os.path.join(path_dir, "__init__.py")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                if not os.path.isfile(path_init):
                    with open(path_init, "wt+") as fi:
                        fi.write("")
                with open(path, 'wt') as fm:
                    fm.writelines(template(tpl_map[class_type.__name__], {
                        "data": create_template_params,
                        "name": self.class_name,
                    }))
                for i in range(10):  # because fs has cache time
                    try:
                        mdl = import_module(self.module)
                        break
                    except ModuleNotFoundError:
                        invalidate_caches()
                        sleep(i)
            else:
                raise
        tp = getattr(mdl, self.class_name)
        if not issubclass(tp, class_type):
            ValueError(f'class {self.class_name} is not subclass of {class_type.__name__}')
        return tp
