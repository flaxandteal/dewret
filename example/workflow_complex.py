"""Complex workflow.

Useful as an example of a workflow with a nested task.

```sh
$ python -m dewret workflow_complex.py --pretty nested_workflow
```
"""

from dewret.tasks import workflow
from workflow_tasks import sum, double, increase

STARTING_NUMBER: int = 23


@workflow()
def nested_workflow() -> int | float:
    """Creates a complex workflow with a nested task.

    Workflow Steps:
    1. **Increase**: The starting number (`STARTING_NUMBER`) is incremented by 1 using the `increase` task.
    2. **Double**: The result from the first step is then doubled using the `double` task.
    3. **Increase Again**: Separately, the number 17 is incremented twice using the `increase` task.
    4. **Sum**: Finally, the results of the two branches (left and right) are summed together using the `sum` task.

    Returns:
    - `int | float`: The result of summing the doubled and increased values, which may be an integer or a float depending on the operations.

    """
    left = double(num=increase(num=STARTING_NUMBER))
    right = increase(num=increase(num=17))
    return sum(left=left, right=right)
