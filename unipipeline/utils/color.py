
COLOR_MAGENTA = '\u001b[35m'
COLOR_YELLOW = '\u001b[33m'
COLOR_GREEN = '\u001b[32m'
COLOR_WHITE = '\u001b[37m'
COLOR_BLACK = '\u001b[30m'
COLOR_RESET = '\u001b[0m'
COLOR_GRAY = '\u001b[0;37m'
COLOR_BLUE = '\u001b[34m'
COLOR_CYAN = '\u001b[36m'
COLOR_RED = '\u001b[31m'


def color_it(clr: str, msg: str) -> str:
    return f'{clr}{msg}{COLOR_RESET}'
