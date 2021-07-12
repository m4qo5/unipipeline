import pytest

from unipipeline.utils.color import color_it, COLOR_RED


@pytest.mark.parametrize('color,text,expected', [
    (COLOR_RED, 'some', '\u001b[31msome\u001b[0m')
])
def test_color(color, text, expected) -> None:
    assert color_it(color, text) == expected
