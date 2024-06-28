# Quickstart

## Installation

From a cloned repository:

    pip install -e .

## Usage

You can render a simple Common Workflow Language [CWL](https://www.commonwl.org/) workflow from a graph composed of one or more tasks as follows:

```python
# workflow.py

from dewret.tasks import task

@task()
def increment(num: int) -> int:
    return num + 1
```

```sh
$ python -m dewret --pretty workflow.py increment num:3
```

```yaml
class: Workflow
cwlVersion: 1.2
outputs:
  out:
    outputSource: increment-e138626779553199eb2bd678356b640f-num
    type: int
steps:
  increment-e138626779553199eb2bd678356b640f-num
    in:
      num:
        default: 3
    out:
    - out
    run: increment
```

By default `dewret` uses a [dask](https://www.dask.org/) backend so that `dewret.task` wraps a `dask.delayed`, and renders a CWL workflow. 


### Programmatic Usage

Building and rendering may be done programmatically,
which provides the opportunity to use custom renderers
and backends, as well as bespoke serialization or formatting.

```python
>>> import sys
>>> import yaml
>>> from dewret.tasks import task, construct
>>> from dewret.renderers.cwl import render
>>> 
>>> @task()
... def increment(num: int) -> int:
...     return num + 1
>>>
>>> result = increment(num=3)
>>> workflow = construct(result, simplify_ids=True)
>>> cwl = render(workflow)
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: 1.2
inputs:
  increment-1-num:
    default: 3
    label: increment-1-num
    type: int
outputs:
  out:
    label: out
    outputSource: increment-1/out
    type: int
steps:
  increment-1:
    in:
      num:
        source: increment-1-num
    out:
    - out
    run: increment

```
