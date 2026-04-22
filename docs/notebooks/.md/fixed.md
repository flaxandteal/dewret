---
jupyter:
  jupytext:
    formats: ipynb,.md//md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.2
  kernelspec:
    display_name: 0.12-env
    language: python
    name: python3
---

# Fixed



## Working examples



### global annotation (example from tests)


```python
from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from dewret.annotations import Fixed

list_2: Fixed[list[int]] = [0, 1, 2, 3]

@workflow()
def loop_over_lists(list_1: list[int]) -> list[int]:
    result = []
    for a, b in zip(list_1, list_2, strict=False):
        result.append(a + b + len(list_2))
    return result


result = loop_over_lists(list_1=[10, 20, 30, 40])
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```

### parameter annotation


```python
from dewret.renderers.cwl import render
from dewret.tasks import construct, workflow
from dewret.annotations import Fixed

@workflow()
def loop_over_lists(list_1: Fixed[list[int]]) -> list[int]:
    result = []
    for a, b in zip(list_1, list_1, strict=False):
        result.append(a + b + len(list_1))
    return result


result = loop_over_lists(list_1=[10, 20, 30, 40])
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```

### simple iteration of a task over a list



This example shows the main use of the Fixed annotation


```python
""" from dewret.renderers.cwl import render
from dewret.tasks import construct, workflow, task
from dewret.annotations import Fixed

@task()
def work(arg: int) -> int:
    # do work
    return arg # need to return something or the loop is optimized away

@workflow()
def loop_work(list: Fixed[list[int]]) -> list[int]:
    # result = []
    # for i in list:
    #     work(arg = i)
    #     result.append(i) 
    result = [work(arg = i) for i in list]

    return result

result = loop_work(list=[1,2])
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl """
```

```python
cwl.keys()
```

```python
cwl['loop_work-1']['steps']
```

### Render time loops don't have to be annotated as `Fixed`


```python
from dewret.renderers.cwl import render
from dewret.tasks import construct, workflow, task
from dewret.annotations import AtRender

list_2: AtRender[list[int]] = [0, 1, 2, 3]

# This task is needed to ensure something gets rendered, note the type isn't enforced
@task()
def iden(arg: str) -> str:
    return arg

@workflow()
def loop_over_lists() -> list[int]:
    result = []
    for a, b in zip(list_2, list_2, strict=False):
        result.append(a + b + len(list_2))
    return iden(arg = result)

result = loop_over_lists()
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```

```python
from dewret.renderers.cwl import render
from dewret.tasks import construct, workflow, task
from dewret.annotations import AtRender

@task()
def work(arg: int) -> int:
    # do work
    return arg # need to return something or the loop is optimized away

@workflow()
def loop_work(list: AtRender[list[int]]) -> list[int]:
    result = []
    for i in list:
        res = work(arg = i)
        result.append(res) 

    return result

result = loop_work(list=[1,2])
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```

```python
cwl['loop_work-1']['steps']
```

## Not working, as designed



Error message is helpful


```python
from dewret.renderers.cwl import render
from dewret.tasks import construct, workflow, task

list_2: list[int] = [0, 1, 2, 3]

@task()
def iden(arg: str) -> str:
    return arg

@workflow()
def loop_over_lists() -> list[int]:
    result = []
    for a, b in zip(list_2, list_2, strict=False):
        result.append(a + b + len(list_2))
    return iden(arg = result)

result = loop_over_lists()
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```
