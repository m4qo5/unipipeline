from datetime import timedelta
from typing import Union, Dict, Any, List, Tuple, Type, TYPE_CHECKING, Optional

from pydantic import BaseModel

from unipipeline.message.uni_message import UniMessage

if TYPE_CHECKING:
    from unipipeline.worker.uni_worker import UniWorker


TUniSendingWorkerUnion = Union[Type['UniWorker[UniMessage, UniMessage]'], str]
TUniSendingMessagePayloadUnion = Union[Dict[str, Any], UniMessage, List[Dict[str, Any]], Tuple[Dict[str, Any], ...], List[UniMessage], Tuple[UniMessage, ...]]


class UniSendingParams(BaseModel):
    alone: bool = False
    ttl_s: Optional[int] = None


class UniGettingAnswerParams(BaseModel):
    answer_tll: timedelta
    alone: bool = False


default_sending_params: UniSendingParams = UniSendingParams(alone=False, ttl_s=None)
default_getting_answer_params: UniGettingAnswerParams = UniGettingAnswerParams(answer_tll=timedelta(seconds=5))
