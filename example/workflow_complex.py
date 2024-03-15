"""Complex workflow.

Useful as an example of a simple workflow.

```sh
$ python -m dewret workflow_complex.py --pretty run
```
"""

from dewret.workflow import Lazy
from dewret.tasks import nested_task
from extra import sum, double, increase

STARTING_NUMBER: int = 23

@nested_task()
def run() -> int | float:
    """Creates a graph of task calls."""
    left = double(num=increase(num=STARTING_NUMBER))
    right = increase(num=increase(num=17))
    return sum(
        left=left,
        right=right
    )
