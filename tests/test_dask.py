from dewret.backends.dask import task, run

@task
def inc_task(base):
    return 1 + base

def test_can_run_task():
    incremented = run(inc_task, 3)
    assert incremented == 4
