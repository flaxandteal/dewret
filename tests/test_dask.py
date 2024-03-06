"""Test the dask backend."""

from typing import cast
from dewret.workflow import Lazy
from dewret.backends.dask import lazy, run

def inc_task(base: int) -> int:
    """Increments a value by one and returns it."""
    return 1 + base

def test_can_run_task() -> None:
    """Check whether a dask task can run via `TaskManager`."""
    task: Lazy = lazy(inc_task)(base=3)
    incremented: int = cast(int, run(None, task))
    assert incremented == 4
