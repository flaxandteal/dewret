"""Testing example renderer.

Correctly fails to import to show a broken module being handled.
"""

from typing import Unpack, TypedDict
from dewret.workflow import Workflow

# This lacking a relative import, while extra itself
# uses one is what breaks the module. It cannot be both
# a package and not-a-package. This is importable by
# adding a . before extra.
from extra import JUMP  # type: ignore


class UnfrenderRendererConfiguration(TypedDict):
    allow_complex_types: bool


def default_config() -> UnfrenderRendererConfiguration:
    return {"allow_complex_types": True}


def render_raw(
    workflow: Workflow, **kwargs: Unpack[UnfrenderRendererConfiguration]
) -> dict[str, str]:
    return {"JUMP": str(JUMP)}
