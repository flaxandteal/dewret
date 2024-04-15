"""Test for expected errors."""

import pytest
from dewret.workflow import Lazy
from dewret.backends.backend_dask import lazy, run

def add_task(left: int, right: int) -> int:
    """Adds two values and returns the result."""
    return left + right

def test_missing_arguments_throw_error() -> None:
    """Check whether a dask task can run via `TaskManager`."""

    task: Lazy = lazy(add_task)(left=3)
    with pytest.raises(TypeError):
        run(None, task)
