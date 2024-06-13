"""Complex workflow.

Useful as an example of a workflow with a nested task.

```sh
$ python -m dewret workflow_complex.py --pretty nested_workflow
```
"""

from dewret.tasks import nested_task
from workflow_tasks import sum, double, increase

STARTING_NUMBER: int = 23


@nested_task()
def nested_workflow() -> int | float:
    """Creates a graph of task calls."""
    left = double(num=increase(num=STARTING_NUMBER))
    right = increase(num=increase(num=17))
    return sum(left=left, right=right)
