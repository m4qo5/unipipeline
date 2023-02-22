from unipipeline.utils.filter_camel import camel_case
from unipipeline.utils.uni_util_color import UniUtilColor
from unipipeline.utils.uni_util_template import UniUtilTemplate


class UniUtil:
    def __init__(self) -> None:
        self._color = UniUtilColor()
        self._template = UniUtilTemplate()
        self._template.set_filter('camel', camel_case)

    @property
    def color(self) -> UniUtilColor:
        return self._color

    @property
    def template(self) -> UniUtilTemplate:
        return self._template
