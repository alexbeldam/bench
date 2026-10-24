"""Import order is the default order of the suite."""
from . import tests_inmem, tests_misc, tests_real  # noqa: F401
from .core import REG, T, selected  # noqa: F401
