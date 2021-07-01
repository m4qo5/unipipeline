import logging


children_loggers = []


def getChild(name: str) -> logging.Logger:
    children_loggers.append(name)
    return logging.getLogger(name)
