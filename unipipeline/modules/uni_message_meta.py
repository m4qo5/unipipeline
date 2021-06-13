import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from uuid import UUID

from pydantic import BaseModel


class UniMessageMeta(BaseModel):
    id: UUID
    date_created: datetime
    payload: Dict[str, Any]

    parent: Optional[Dict[str, Any]]

    @staticmethod
    def create_new(data: Dict[str, Any]) -> 'UniMessageMeta':
        return UniMessageMeta(
            id=uuid.uuid4(),
            date_created=datetime.now(),
            payload=data,
            parent=None
        )

    def create_child(self, data: Dict[str, Any]) -> 'UniMessageMeta':
        return UniMessageMeta(
            id=uuid.uuid4(),
            date_created=datetime.now(),
            payload=data,
            parent=self.dict()
        )
