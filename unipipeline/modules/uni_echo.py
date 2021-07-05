import logging
import sys
from typing import IO, Union

from unipipeline.utils.color import COLOR_RED, COLOR_CYAN, COLOR_YELLOW, COLOR_GRAY, color_it, COLOR_GREEN

SUPPORTED_LVL = {logging.INFO, logging.DEBUG, logging.WARNING, logging.ERROR}


def get_lvl(lvl: Union[str, int]) -> int:
    if isinstance(lvl, str):
        lvl = lvl.strip().lower()
        if lvl == 'info':
            return logging.INFO
        if lvl == 'debug':
            return logging.DEBUG
        if lvl == 'error':
            return logging.ERROR
        if lvl == 'warning':
            return logging.WARNING
        raise ValueError('invalid value of level')
    assert lvl in SUPPORTED_LVL
    return lvl


class UniEcho:
    def __init__(self, name: str, prefix: str = '', level: Union[int, str] = 'info', colors: bool = True) -> None:
        self._name = name
        self._level = get_lvl(level)
        self._colors = colors

        prefix = f'{f"{prefix} | " if prefix else ""}{self._name}'
        self._debug_prefix = self._color_it(COLOR_GRAY, f'{prefix} | DEBUG   :: ')
        self._info_prefix = self._color_it(COLOR_CYAN, f'{prefix} | INFO    :: ')
        self._warn_prefix = self._color_it(COLOR_YELLOW, f'{prefix} | WARNING :: ')
        self._err_prefix = self._color_it(COLOR_RED, f'{prefix} | ERROR   :: ')
        self._success_prefix = self._color_it(COLOR_GREEN, f'{prefix} :: ')

        self._prefix = prefix

    def _color_it(self, color: str, msg: str) -> str:
        return color_it(color, msg) if self._colors else msg

    def mk_child(self, name: str) -> 'UniEcho':
        e = UniEcho(name, prefix=self._prefix, level=self._level, colors=self._colors)
        return e

    @property
    def level(self) -> int:
        return self._level

    @level.setter
    def level(self, value: Union[int, str]) -> None:
        self._level = get_lvl(value)

    def echo(self, msg: str, stream: IO = sys.stdout) -> None:
        stream.write(f'{msg}\n')

    def log_debug(self, msg: str) -> None:
        if logging.DEBUG >= self._level:
            self.echo(f'{self._debug_prefix}{msg}')

    def log_info(self, msg: str) -> None:
        if logging.INFO >= self._level:
            self.echo(f'{self._info_prefix}{msg}')

    def log_warning(self, msg: str) -> None:
        if logging.WARNING >= self._level:
            self.echo(f'{self._warn_prefix}{msg}', stream=sys.stderr)

    def log_error(self, msg: str) -> None:
        if logging.ERROR >= self._level:
            self.echo(f'{self._err_prefix}{msg}', stream=sys.stderr)

    def exit_with_error(self, msg: str) -> None:
        self.echo(self._color_it(COLOR_RED, f'UNI ERROR :: {msg}'))
        exit(1)

    def success(self, msg: str) -> None:
        self.echo(f'{self._success_prefix}{color_it(COLOR_GREEN, msg)}')
