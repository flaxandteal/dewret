"""Test the dask backend."""

import pytest
from typing import cast
from dewret.workflow import Lazy
from dewret.backends.backend_dask import lazy, run


def inc_task(base: int) -> int:
    """Increments a value by one and returns it."""
    return 1 + base


def add_task(left: int, right: int) -> int:
    """Adds two values and returns the result."""
    return left + right


def test_can_run_task() -> None:
    """Check whether a dask task can run via `TaskManager`."""
    task: Lazy = lazy(inc_task)(base=3)
    incremented: int = cast(int, run(None, task))
    assert incremented == 4


def test_missing_arguments_throw_error_in_dask() -> None:
    """Check whether a dask task can run via `TaskManager`."""
    task: Lazy = lazy(add_task)(left=3)
    with pytest.raises(TypeError):
        run(None, task)
