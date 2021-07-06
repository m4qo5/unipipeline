from typing import Any, Dict

from pydantic import BaseModel

from unipipeline.utils.parse_definition import ParseDefinitionError


class UniDefinition(BaseModel):
    name: str

    dynamic_props_: Dict[str, Any]

    def configure_dynamic(self, defaults: Dict[str, Any]) -> Dict[str, Any]:
        res = dict()
        for k, v in defaults.items():
            if k in self.dynamic_props_:
                res[k] = self.dynamic_props_[k]
                if v is not None and not isinstance(res[k], type(v)):
                    raise ParseDefinitionError(f'configure broker {self.name}->{k} has invalid property type. must be {type(v)}')
            else:
                res[k] = v
        return res
