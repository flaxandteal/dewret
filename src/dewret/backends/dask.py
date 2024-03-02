from dask import delayed

task = delayed
def run(task, *args, **kwargs):
    return task(*args, **kwargs)
