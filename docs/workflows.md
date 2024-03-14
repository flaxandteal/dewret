# Workflows

## Setup

We can pull in dewret tools to produce CWL with a small number of imports.

```python
>>> import sys
>>> import yaml
>>> from dewret.tasks import task, run
>>> from dewret.renderers.cwl import render

## Dependencies

Specifying step interdependencies is possible by combining lazy-evaluated function
calls.

Dewret hashes the parameters to identify and unify steps. This lets you do, for example:

```mermaid
graph TD
    A[increment] --> B[double]
    A[increment] --> C[mod10]
    B[double] --> D[sum]
    C[mod10] --> D[sum]
```

In code, this would be:

```python
>>> @task()
... def increment(num: int) -> int:
...     """Increment an integer."""
...     return num + 1
>>> 
>>> @task()
... def double(num: int) -> int:
...     """Double an integer."""
...     return 2 * num
>>> 
>>> @task()
... def mod10(num: int) -> int:
...     """Double an integer."""
...     return num % 10
>>> 
>>> @task()
... def sum(left: int, right: int) -> int:
...     """Add two integers."""
...     return left + right
>>>
>>> result = sum(
...     left=double(num=increment(num=23)),
...     right=mod10(num=increment(num=23))
... )
>>> workflow = run(result, simplify_ids=True)
>>> cwl = render(workflow)
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: 1.2
inputs: {}
outputs:
  out:
    outputSource: sum-1/out
    type: string
steps:
  double-1:
    in:
      num:
        source: increment-1/out
    out:
    - out
    run: double
  increment-1:
    in:
      num:
        default: 23
    out:
    - out
    run: increment
  mod10-1:
    in:
      num:
        source: increment-1/out
    out:
    - out
    run: mod10
  sum-1:
    in:
      left:
        source: double-1/out
      right:
        source: mod10-1/out
    out:
    - out
    run: sum

```

## Parameters

The tool will spot global variables that you have used when building your tasks,
and treat them as parameters. It will try to get the type from the typehint, or
the value that you have set it to. This only works for basic types (and dict/lists of
those).

For example:
```python
>>> INPUT_NUM = 3
>>> @task()
... def rotate(num: int) -> int:
...    """Rotate an integer."""
...    return (num + INPUT_NUM) % INPUT_NUM
>>>
>>> result = rotate(num=3)
>>> workflow = run(result, simplify_ids=True)
>>> cwl = render(workflow)
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: 1.2
inputs:
  INPUT_NUM:
    label: INPUT_NUM
    type: int
outputs:
  out:
    outputSource: rotate-1/out
    type: string
steps:
  rotate-1:
    in:
      INPUT_NUM:
        source: INPUT_NUM
      num:
        default: 3
    out:
    - out
    run: rotate

```

## Nested tasks

When you wish to combine tasks together programmatically,
you can use nested tasks. These are run at _render_ time, not
execution time. In other words, they do not appear in the
final graph, and so must only combine other tasks.

For example:
```python
>>> from dewret.tasks import nested_task
>>> @nested_task()
... def double_rotate(num: int) -> int:
...    """Rotate an integer twice."""
...    return rotate(num=rotate(num=num))
>>>
>>> result = double_rotate(num=3)
>>> workflow = run(result, simplify_ids=True)
>>> cwl = render(workflow)
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: 1.2
inputs:
  INPUT_NUM:
    label: INPUT_NUM
    type: int
outputs:
  out:
    outputSource: rotate-2/out
    type: string
steps:
  rotate-1:
    in:
      INPUT_NUM:
        source: INPUT_NUM
      num:
        default: 3
    out:
    - out
    run: rotate
  rotate-2:
    in:
      INPUT_NUM:
        source: INPUT_NUM
      num:
        source: rotate-1/out
    out:
    - out
    run: rotate

```
