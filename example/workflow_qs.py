"""Quickstart workflow.

Useful as an example of a simple workflow.

```sh
$ python -m dewret workflow_qs.py --pretty increment num:3 --backend DASK
```
"""

from dewret.tasks import task


@task()
def increment(num: int) -> int:
    """Add 1 to a number."""
    return num + 1
