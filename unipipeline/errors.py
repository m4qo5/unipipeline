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
