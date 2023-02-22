from typing import Type

from unipipeline.definitions.uni_module_definition import UniModuleDefinition
from unipipeline.message.uni_message import UniMessage

uni_message_template = '''from unipipeline import UniMessage


class {{name}}(UniMessage):
    pass

'''


class UniModuleDefinitionMessage(UniModuleDefinition[Type[UniMessage]]):
    ASSIGNED_TYPE = UniMessage
    CREATE_TEMPLATE = uni_message_template
