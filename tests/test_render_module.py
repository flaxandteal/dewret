"""Check renderers can be imported live."""

import pytest
from pathlib import Path
from dewret.tasks import construct
from dewret.render import get_render_method

from ._lib.extra import increment, triple_and_one


def test_can_load_render_module() -> None:
    """Checks if we can load a render module."""
    result = triple_and_one(num=increment(num=3))
    workflow = construct(result, simplify_ids=True)
    workflow._name = "Fred"

    frender_py = Path(__file__).parent / "_lib/frender.py"
    render = get_render_method(frender_py)

    assert render(workflow) == {
        "__root__": """
I found a workflow called Fred.
It has 2 steps!
They are:
* Something called increment-1

* A portal called triple_and_one-1 to another workflow,
  whose name is triple_and_one

It probably got made with JUMP=1.0
""",
        "triple_and_one-1": """
I found a workflow called triple_and_one.
It has 3 steps!
They are:
* Something called double-1-1

* Something called sum-1-1

* Something called sum-1-2

It probably got made with JUMP=1.0
""",
    }

def test_get_correct_import_error_if_unable_to_load_render_module() -> None:
    """TODO: Docstrings."""
    unfrender_py = Path(__file__).parent / "_lib/unfrender.py"
    with pytest.raises(ModuleNotFoundError) as exc:
      get_render_method(unfrender_py)

    entry = exc.traceback[-1]
    assert Path(entry.path).resolve() == (
        Path(__file__).parent / "_lib" / "unfrender.py"
    ).resolve()
    assert entry.relline == 12
    assert "No module named 'extra'" in str(exc.value)

    nonfrender_py = Path(__file__).parent / "_lib/nonfrender.py"
    with pytest.raises(NotImplementedError) as nexc:
      get_render_method(nonfrender_py)

    assert "This render module neither seems to be a structured nor a raw render module" in str(nexc.value)
