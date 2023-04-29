from typing import Optional


class UniError(Exception):
    pass


class UniAnswerDelayError(UniError):
    pass


class UniRedundantAnswerError(UniError):
    pass


class UniEmptyAnswerError(UniError):
    pass


class UniConfigError(UniError):
    pass


class UniDefinitionNotFoundError(UniError):
    pass


class UniMessageError(UniError):
    pass


class UniMessagePayloadParsingError(UniMessageError):
    pass


class UniAnswerMessagePayloadParsingError(UniMessageError):
    pass


class UniPayloadSerializationError(UniMessageError):
    pass


class UniSendingToUndefinedWorkerError(UniError):
    pass


class UniMessageRejectError(UniError):

    def __init__(self, exc: Optional[Exception] = None) -> None:
        self._exc = exc

    @property
    def rejection_exception(self) -> Optional[Exception]:
        return self._exc
