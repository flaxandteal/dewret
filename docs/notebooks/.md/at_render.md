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

### AtRender annotations with arguments tells dewret to evalute those arguments at render time, input ends up capitalized in the rendered output


```python
from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from dewret.annotations import AtRender

@task()
def concat_strings(str1: str, str2: str) -> str:
    return str1 + str2

@workflow()
def create_greeting_in_all_caps(prefix: AtRender[str], name: AtRender[str]) -> str:
    prefix_cap = prefix.upper()
    name_cap = name.upper()
    return concat_strings(str1=prefix_cap, str2=name_cap)

result = create_greeting_in_all_caps(prefix="Hello to ", name="John")
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```

### We can also and optionally pass these as global parameters which is perhaps more explicit in the programming intent


```python
from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from dewret.annotations import AtRender

@task()
def concat_strings(str1: str, str2: str) -> str:
    return str1 + str2

global_prefix: AtRender[str] = "Hello to "
global_name: AtRender[str] = "John"

@workflow()
def create_greeting_in_all_caps() -> str:
    prefix_cap = global_prefix.upper()
    name_cap = global_name.upper()
    return concat_strings(str1=prefix_cap, str2=name_cap)

result = create_greeting_in_all_caps()
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```

### If one creates a `@task` the capitalizing will work at run time.


```python
from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow

@task()
def concat_strings(str1: str, str2: str) -> str:
    return str1 + str2

@task()
def caps(arg: str) -> str:
    return arg.upper()

@workflow()
def create_greeting_in_all_caps(prefix: str, name: str) -> str:
    prefix_cap = caps(arg=prefix)
    name_cap = caps(arg=name)
    return concat_strings(str1=prefix_cap, str2=name_cap)


result = create_greeting_in_all_caps(prefix="Hello to ", name="John")

workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```

### With no annotations, rendering will fail, with a somewhat cryptic message that should be improved


```python
from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow

@task()
def concat_strings(str1: str, str2: str) -> str:
    return str1 + str2

@task()
def caps(arg: str) -> str:
    return arg.upper()

@workflow()
def create_greeting_in_all_caps(prefix: str, name: str) -> str:
    prefix_cap = caps(arg=prefix)
    name_cap = caps(arg=name)
    return concat_strings(str1=prefix_cap, str2=name_cap)


result = create_greeting_in_all_caps(prefix="Hello to ", name="John")

workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
cwl
```
