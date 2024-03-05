"""Test the dask backend."""

from dewret.backends.dask import lazy, run

@lazy
def inc_task(base: int) -> int:
    """Increments a value by one and returns it."""
    return 1 + base

def test_can_run_task() -> None:
    """Check whether a dask task can run via `TaskManager`."""
    incremented = run(None, inc_task(base=3))
    assert incremented == 4
