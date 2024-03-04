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
steps:
  increment-012ef3b3ffb9d15c3f2837aa4bb20a8d:
    in:
      num:
        default: 3
    run: increment
```
