import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from uuid import UUID

from pydantic import BaseModel


class UniMessageMetaErrTopic(Enum):
    SYSTEM_ERR = 'error_system'
    MESSAGE_PAYLOAD_ERR = 'error_message_payload'
    HANDLE_MESSAGE_ERR = 'error_message_handling'
    ERROR_HANDLING_ERR = 'error_handling'


class UniMessageMetaErr(BaseModel):
    error_topic: UniMessageMetaErrTopic
    error_type: str
    error_message: str
    retry_times: int


class UniMessageMeta(BaseModel):
    id: UUID
    date_created: datetime
    payload: Dict[str, Any]

    parent: Optional[Dict[str, Any]]
    error: Optional[UniMessageMetaErr]

    @property
    def has_error(self) -> bool:
        return self.error is not None

    @staticmethod
    def create_new(data: Dict[str, Any], error: Optional[UniMessageMetaErr] = None) -> 'UniMessageMeta':
        return UniMessageMeta(
            id=uuid.uuid4(),
            date_created=datetime.now(),
            payload=data,
            parent=None,
            error=error,
        )

    def create_child(self, payload: Dict[str, Any]) -> 'UniMessageMeta':
        return UniMessageMeta(
            id=uuid.uuid4(),
            date_created=datetime.now(),
            payload=payload,
            parent=self.dict(),
            error=None,
        )

    def create_error_child(self, error_topic: UniMessageMetaErrTopic, error: Exception) -> 'UniMessageMeta':
        return UniMessageMeta(
            id=self.id,
            date_created=self.date_created,
            payload=self.payload,
            parent=self.dict(),
            error=UniMessageMetaErr(
                error_topic=error_topic,
                error_type=type(error).__name__,
                error_message=str(error),
                retry_times=self.error.retry_times + 1 if self.error is not None else 0
            ),
        )
