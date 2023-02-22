from typing import Type

from unipipeline.definitions.uni_module_definition import UniModuleDefinition
from unipipeline.waiting.uni_wating import UniWaiting


uni_waiting_template = '''from unipipeline import UniWaiting


class {{name}}(UniWaiting):
    def try_to_connect(self) -> None:
        raise NotImplementedError('method try_to_connect must be specified for class "{{name}}"')

'''


class UniModuleDefinitionWaiting(UniModuleDefinition[Type[UniWaiting]]):
    ASSIGNED_TYPE = UniWaiting
    CREATE_TEMPLATE = uni_waiting_template
