from dewret.backends.dask import lazy, run

@lazy
def inc_task(base):
    return 1 + base

def test_can_run_task():
    incremented = run(None, inc_task(base=3))
    assert incremented == 4
