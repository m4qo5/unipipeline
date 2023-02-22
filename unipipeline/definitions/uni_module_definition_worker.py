from typing import Type, Any

from unipipeline.definitions.uni_module_definition import UniModuleDefinition
from unipipeline.worker.uni_worker import UniWorker


uni_worker_template = '''from unipipeline import UniWorker, UniWorkerConsumerMessage

from {{data.input_message.type.module}} import {{data.input_message.type.object_name}}


class {{name}}(UniWorker[{{data.input_message.type.object_name}}, None]):
    def handle_message(self, msg: UniWorkerConsumerMessage[{{data.input_message.type.object_name}}]) -> None:
        raise NotImplementedError('method handle_message must be specified for class "{{name}}"')

'''


class UniModuleDefinitionWorker(UniModuleDefinition[Type[UniWorker[Any, Any]]]):
    ASSIGNED_TYPE = UniWorker
    CREATE_TEMPLATE = uni_worker_template
