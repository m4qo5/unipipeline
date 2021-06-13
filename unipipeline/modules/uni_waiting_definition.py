import logging
from time import sleep

from pydantic import BaseModel

from unipipeline.modules.uni_module_definition import UniModuleDefinition
from unipipeline.modules.uni_wating import UniWaiting

logger = logging.getLogger(__name__)


class UniWaitingDefinition(BaseModel):
    name: str
    retry_max_count: int
    retry_delay_s: int
    type: UniModuleDefinition

    def wait(self) -> None:
        waiting_type = self.type.import_class(UniWaiting)
        for try_count in range(self.retry_max_count):
            try:
                w = waiting_type()
                w.try_to_connect()
                logger.info('%s is available in inu', waiting_type.__name__)
                return
            except Exception:
                logger.debug('retry wait for %s [%s/%s]', waiting_type.__name__, try_count, self.retry_max_count)
                sleep(self.retry_delay_s)
                continue
        raise ConnectionError('unavailable connection to %s', waiting_type.__name__)
