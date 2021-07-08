from pydantic import ValidationError


class UniError(Exception):
    pass


class UniDefinitionNotFoundError(UniError):
    pass


class UniConfigError(UniError):
    pass


class UniPayloadError(UniError):
    pass


class UniSendingToWorkerError(UniError):
    pass


class UniWorkFlowError(UniError):
    pass


class UniAnswerDelayError(UniError):
    pass
