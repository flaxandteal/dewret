"""Testing example renderer.

Correctly fails to import to show a broken module being handled.
"""

from typing import Unpack, TypedDict
from dewret.workflow import Workflow

from .extra import JUMP


class NonfrenderRendererConfiguration(TypedDict):
    allow_complex_types: bool


# This should fail to load as default_config is not present. However it would
# ignore the fact that the return type is not a (subtype of) dict[str, RawType]
# def default_config() -> int:
#     return 3


def render_raw(
    workflow: Workflow, **kwargs: Unpack[NonfrenderRendererConfiguration]
) -> dict[str, str]:
    return {"JUMP": str(JUMP)}
