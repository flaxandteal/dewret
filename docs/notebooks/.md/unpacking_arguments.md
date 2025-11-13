---
jupyter:
  jupytext:
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

# Unpacking arguments



Unpacking syntax works for passing arguments to `@task`s


```python
from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from typing import List

@task()
def f(a: int, b: int, c: int) -> List[int]:
    return [a, b, c]

@workflow()
def foo() -> List[int]:
    return f(**{'a':1, 'b': 2, 'c': 3})

result = foo()

workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```

Unpacking syntax can also be used in signature of a `@task`


```python
from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from typing import TypedDict
from typing import Unpack

class MyArgs(TypedDict):
    a: int
    b: int

@task()
# @workflow()
def g(**args: Unpack[MyArgs]) -> str:
    # Unpack the dictionary into a new dictionary with string keys
    temp: dict[str, int] = dict(args)

    return temp['a']

@workflow()
def foo() -> int:
    return g(**{'a':1, 'b': 2})

result = foo()

workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```
