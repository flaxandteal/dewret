# Quickstart

## Installation

From a cloned repository:

    pip install -e .

## Usage

You can render a simple [CWL](https://www.commonwl.org/) workflow from a [dask](https://www.dask.org/) delayed graph as follows:

```python
# workflow.py

from dewret.tasks import task

@task()
def increment(num: int):
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
    outputSource: increment-012ef3b3ffb9d15c3f2837aa4bb20a8d/out
    type: string
steps:
  increment-012ef3b3ffb9d15c3f2837aa4bb20a8d:
    in:
      num:
        default: 3
    out:
    - out
    run: increment
```

### Programmatic Usage

Building and rendering may be done programmatically,
which provides the opportunity to use custom renderers
and backends, as well as bespoke serialization or formatting.

```python
>>> import sys
>>> import yaml
>>> from dewret.tasks import task, run
>>> from dewret.renderers.cwl import render
>>> 
>>> @task()
... def increment(num: int):
...     return num + 1
>>>
>>> result = increment(num=3)
>>> workflow = run(result)
>>> cwl = render(workflow)
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: 1.2
inputs: {}
outputs:
  out:
    outputSource: increment-012ef3b3ffb9d15c3f2837aa4bb20a8d/out
    type: string
steps:
  increment-012ef3b3ffb9d15c3f2837aa4bb20a8d:
    in:
      num:
        default: 3
    out:
    - out
    run: increment

```
