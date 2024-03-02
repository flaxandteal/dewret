from dask import delayed

lazy = delayed
def run(workflow, task):
    return task.compute(__workflow__=workflow)
