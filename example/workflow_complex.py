"""Complex workflow.

Useful as an example of a workflow with a nested task.

```sh
$ python -m dewret workflow_complex.py --pretty run
```
"""

from dewret.workflow import Lazy
from dewret.tasks import nested_task
from dask.array import sin
from extra import sum, double, increase
from dask import delayed

STARTING_NUMBER: int = 23

@nested_task()
def nested_workflow() -> int | float:
    """Creates a graph of task calls."""
    left = double(num=increase(num=STARTING_NUMBER))
    left = left + 1
    left = sin(left * 2)
    right = increase(num=increase(num=left))
    right = right * 2 + left
    return sum(
        left=left,
        right=right
    )
