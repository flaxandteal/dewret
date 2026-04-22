# Writing a Workflow <!-- omit in toc -->
- [Description](#description)
- [Imports](#imports)
- [Dewret decorators](#dewret-decorators)
- [Parameters](#parameters)
- [Chaining steps together and caching of steps](#chaining-steps-together-and-caching-of-steps)
- [Render-time vs execution-time](#render-time-vs-execution-time)
  - [Global annotation](#global-annotation)
  - [Annotation in function (`@task`) signature](#annotation-in-function-task-signature)
  - [Import for render time function calls](#import-for-render-time-function-calls)
- [`Fixed` and looping over lists](#fixed-and-looping-over-lists)
- [Nested tasks](#nested-tasks)
- [Output from steps](#output-from-steps)
- [Chaining workflows together](#chaining-workflows-together)
- [Complex input types and factories](#complex-input-types-and-factories)
  - [Input factories as task](#input-factories-as-task)
  - [Input factories as a parameter](#input-factories-as-a-parameter)

## Description

A dewret workflow is composed of one or more steps that may make use of both local and global parameters. Each step is defined by a dewret task that is created by using the `@task()` decorator, and each task may be used by multiple steps.

Programming a workflow in dewret looks very similar to vanilla Python. Dewret has an intuitive execution model and syntax: code has to be lightly annotated and steps consist of normal functions that have been decorated.

The output of Dewret is a static representation of a computational graph, in yaml, of connected steps (their names, to be resolved by the worflow engine) along with their static inputs.

<!-- This diagram was drawn by first getting a version on canva, then using an LLM to get some code, and then tweaking it, it doesn't display well on markdown preview on vscode but it displays well on github -->
```mermaid
graph LR;
    A["<b>my_workflow.py</b><br>Lightly Annotated Python"]
    B(Dewret)
    C["<b>my_workflow.yaml</b><br>Static Rendered Workflow"]
    D["Workflow language<br>specific<br>renderer - e.g.<br>CWL"]
    E{Execute Workflow}

    A --> B
    B --> C
    C -- Workflow Engine --> E
    D --> B

    style A fill:#e0d8f7,stroke:#e0d8f7,stroke-width:1px,color:#000
    style B fill:#fff,stroke:#fff,stroke-width:0px,color:#000
    style C fill:#faf3bf,stroke:#faf3bf,stroke-width:1px,color:#000
    style D fill:#cdeaf7,stroke:#cdeaf7,stroke-width:1px,color:#000
    style E fill:#e88080,stroke:#d14949,stroke-width:1px,color:#000
```

## Imports

We can pull in dewret tools to produce CWL with a small number of imports.

```python
>>> import sys
>>> import yaml
>>> from dewret.tasks import task, construct
>>> from dewret.workflow import param
>>> from dewret.renderers.cwl import render

```

## Dewret decorators

Dewret uses the following decorators to mark functions as steps to be evaluated by the workflow engine:

* `@task()`: basib building step that defines a step
* `@workflow()`: defines the entry point for the workflow
* [`@factory()`](): allows for complex inputs to be created at run time.

We will refer to these as the dewret decorators

## Parameters

Dewret will spot global variables that you have used when building your tasks,
and treat them as parameters. It will try to get the type from the typehint, or
the value that you have set it to. This only works for basic types (and dict/lists of
those).

While global variables are implicit input to the Python function **note that**:

1. in CWL, they will be rendered as explicit global input to a step
2. as input, they are read-only, and must not be updated

For example:
```python
>>> import sys
>>> import yaml
>>> from dewret.workflow import param
>>> from dewret.tasks import task, construct
>>> from dewret.renderers.cwl import render
>>> INPUT_NUM = 3
>>> @task()
... def rotate(num: int) -> int:
...    """Rotate an integer."""
...    return (num + INPUT_NUM) % INPUT_NUM
>>>
>>> result = rotate(num=5)
>>> wkflw = construct(result, simplify_ids=True)
>>> cwl = render(wkflw)["__root__"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs:
  INPUT_NUM:
    default: 3
    label: INPUT_NUM
    type: int
  rotate-1-num:
    default: 5
    label: num
    type: int
outputs:
  out:
    label: out
    outputSource: rotate-1/out
    type: int
steps:
  rotate-1:
    in:
      INPUT_NUM:
        source: INPUT_NUM
      num:
        source: rotate-1-num
    out:
    - out
    run: rotate

```
## Chaining steps together and caching of steps

The output of one `@task()` can be the input of another one. 
Steps in the rendered output yaml are not guaranteed to be in order of execution.
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
>>> import sys
>>> import yaml
>>> from dewret.tasks import task, construct
>>> from dewret.renderers.cwl import render
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
...     """Take num mod 10."""
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
>>> wkflw = construct(result, simplify_ids=True)
>>> cwl = render(wkflw)["__root__"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs:
  increment-1-num:
    default: 23
    label: num
    type: int
outputs:
  out:
    label: out
    outputSource: sum-1/out
    type: int
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
        source: increment-1-num
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

Notice two things:

* `@workflow()`s are equivalent to `@task()`s;  `@task()` can be used as the entry point to a workflow (`sum` in this case).
* The `increment` tasks appears twice in the CWL workflow definition, being referenced twice in the python code above. 
This duplication can be avoided by explicitly indicating that the parameters are the same, with the `param` function.

```python
>>> import sys
>>> import yaml
>>> from dewret.workflow import param
>>> from dewret.tasks import task, construct
>>> from dewret.renderers.cwl import render
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
...     """Take num mod 10."""
...     return num % 10
>>> 
>>> @task()
... def sum(left: int, right: int) -> int:
...     """Add two integers."""
...     return left + right
>>>
>>> num = param("num", default=3)
>>> result = sum(
...     left=double(num=increment(num=num)),
...     right=mod10(num=increment(num=num))
... )
>>> wkflw = construct(result, simplify_ids=True)
>>> cwl = render(wkflw)["__root__"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs:
  num:
    default: 3
    label: num
    type: int
outputs:
  out:
    label: out
    outputSource: sum-1/out
    type: int
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
        source: num
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

## Render-time vs execution-time

* See this [notebook](notebooks/at_render.ipynb) for more examples.

Unlike normal Python code, Dewret code is designed to be compiled (transpiled) to an intermediate representation which is run by a third party workflow engine. Analogous to other compiled languages, Dewret has a way to specify whether code will run at compilation time (by Python at "rendering" time in Dewret jargon) or workflow execution time (by the workflow engine).

* The main mechanism for controlling whether an expression is evaluated at render time is the `AtRender` annotation.
* When calling a function we wish to evaluate at render time within a `@task` or `@workflow`, we have to import within the calling `@task` or `@workflow`.

### Global annotation

```py
from dewret.annotations import AtRender

# for a parameter that is consumed as a global variable, the AtRender annotation has to appear when defining the variable
DEBUG: AtRender[bool] = True

@workflow()
def train(...) -> None:
    ...
    # this will fail without the AtRender annotation
    if DEBUG:
      # debug stuff 
      ...
```

### Annotation in function (`@task`) signature

Alternatively, the annotation can be passed as a parameter 

```py
from dewret.annotations import AtRender

@workflow()
def train(debug: AtRender[bool]) -> None:
    ...
    # this will fail without the AtRender annotation
    if debug:
      # debug stuff 
      ...
```

without the annotation, we get an the following error (note that "construction" refers to a substep of the rendering process):

```sh
dewret.tasks.TaskException: This reference, switch, cannot be evaluated during construction.
```

### Import for render time function calls

As workflows represent a graph of functions designed to be run by a workflow engine, If you call a function intended to run at render time (i.e. not a dewret decorator) from within a dewret decorator (to be run at execution time), Dewret will assume you have made a mistake and complain.

If this is indeed what you wanted to you can either:

* Define the function within the dewret decorator itself

```py
from dewret.tasks import task
from dewret.annotations import AtRender

var: AtRender[int] = 1

@task()
def some_task()
  def render_time_fun(int)
    ...
  temp = render_time_fun(var)
```

* Locally import if from another module, within the dewret decorator.

```py
from dewret.tasks import task
from dewret.annotations import AtRender

var: AtRender[int] = 1

@task()
def some_task()
  from utilities import render_time_fun
  
  temp = render_time_fun(var)
  ...
```
The inputs must be annotated with `AtRender`

* See the examples in `docs/demos/render_time_imports`: for a working example run `python import_render_function.py`

## `Fixed` and looping over lists

* See this [notebook](notebooks/fixed.ipynb) for more examples.

As looping over a list can affect the shape of the execution graph it presents a problem when trying to represent the execution graph statically which a requirement for most workflow engines. These lists can be either inputs to the workflow or outputs from other steps

Dewret has a feature to explicitly specify that a list will have a fixed length. The length determines the "shape" of the execution graph which can then be statically rendered, even if we don't know the values of the list at render time.

To instruct Dewret that a list has a fixed length we use the `Fixed` annotation. Similarly to the `AtRender` annotation it can be placed either in a global variable declaration or in the signature of a parameter

```py
from dewret.renderers.cwl import render
from dewret.tasks import construct, workflow, task
from dewret.annotations import Fixed

@task()
def work(arg: int) -> int:
    # do work
    return arg # need to return something or the loop is optimized away

@workflow()
def loop_work(list: Fixed[list[int]]) -> list[int]:
    result = []
    for i in list:
        work(arg = i)
        result.append(i) 

    return result

result = loop_work(list=[1,2])
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
```
The steps look like this:
```py
cwl['loop_work-1']['steps']
```
```json
{
'work-1-1': 
  {'run': 'work', 'in': {'arg': {'source': 'list[1]'}}, 'out': ['out']},
'work-1-2': 
  {'run': 'work', 'in': {'arg': {'source': 'list[0]'}}, 'out': ['out']}
}
```

It is worth noting that if a loop is annotated as `AtRender` it doesn't need to be annotated as `Fixed`:

```py
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
```
As expected the steps in the output don't reference the list any more:

```py
cwl['loop_work-1']['steps']
```
```json
{'work-1-1': {'run': 'work', 'in': {'arg': {'default': 1}}, 'out': ['out']},
 'work-1-2': {'run': 'work', 'in': {'arg': {'default': 2}}, 'out': ['out']}}
```

## Nested tasks

Dewret can handle arbitrarily nested steps as `@task()` decorated functions can call each other within their body.

When you wish to combine tasks together programmatically,
you can use nested tasks. These are run at _render_ time, not
execution time. In other words, they do not appear in the
final graph, and so must only combine other tasks. or contain other render time code.

```python
>>> import sys
>>> import yaml
>>> from dewret.core import set_configuration
>>> from dewret.tasks import task, construct, workflow
>>> from dewret.renderers.cwl import render
>>> INPUT_NUM = 3
>>> @task()
... def rotate(num: int) -> int:
...     """Rotate an integer."""
...     return (num + INPUT_NUM) % INPUT_NUM
>>>
>>> @workflow()
... def double_rotate(num: int) -> int:
...     """Rotate an integer twice."""
...     return rotate(num=rotate(num=num))
>>>
>>> with set_configuration(flatten_all_nested=True):
...     result = double_rotate(num=3)
...     wkflw = construct(result, simplify_ids=True)
...     cwl = render(wkflw)["__root__"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs:
  INPUT_NUM:
    default: 3
    label: INPUT_NUM
    type: int
  num:
    default: 3
    label: num
    type: int
outputs:
  out:
    label: out
    outputSource: rotate-1/out
    type: int
steps:
  rotate-1:
    in:
      INPUT_NUM:
        source: INPUT_NUM
      num:
        source: rotate-2/out
    out:
    - out
    run: rotate
  rotate-2:
    in:
      INPUT_NUM:
        source: INPUT_NUM
      num:
        source: num
    out:
    - out
    run: rotate

```
Note that, as with all dewret calculations, only the steps
necessary to achieve the ultimate output are included in the final
graph. Therefore, nested tasks must return a step execution
(task that is being called) that forces any other calculations
you wish to happen. __In other words, if a task in a
nested task does not have an impact on the return value,
it will disappear__.
For example, the following code renders the same workflow as in the previous example:


```python
@workflow()
def double_rotate(num: int) -> int:
   """Rotate an integer twice."""
   unused_var = increment(num=num)
   return rotate(num=rotate(num=num))
```

## Output from steps

Each step, by default, is treated as having
a single result. However, we allow a mechanism
for specifying multiple fields, using `attrs` or `dataclasses`. 

**Question**: Can one return a list? if so can it be indexed? if not make analogy with kubeflow pipelines

Where needed, fields can be accessed outside of tasks
by dot notation and dewret will map that access to a
specific output field in CWL.

Note that in the example below, `shuffle` is still
only seen once in the graph:

```mermaid
graph TD
    A[shuffle] --> B[hearts]
    A[shuffle] --> C[diamonds]
    B[hearts] --> D[sum]
    C[diamonds] --> D[sum]
```

As code:

```python
>>> import sys
>>> import yaml
>>> from attrs import define
>>> from numpy import random
>>> from dewret.tasks import task, construct
>>> from dewret.renderers.cwl import render
>>> @define
... # @dataclass # works here too
... class PackResult:
...     hearts: int
...     clubs: int
...     spades: int
...     diamonds: int
>>>
>>> @task()
... def shuffle(max_cards_per_suit: int) -> PackResult:
...    """Fill a random pile from a card deck, suit by suit."""
...    return PackResult(
...        hearts=random.randint(max_cards_per_suit),
...        clubs=random.randint(max_cards_per_suit),
...        spades=random.randint(max_cards_per_suit),
...        diamonds=random.randint(max_cards_per_suit)
...    )
>>> @task()
... def sum(left: int, right: int) -> int:
...    return left + right
>>> red_total = sum(
...     left=shuffle(max_cards_per_suit=13).hearts,
...     right=shuffle(max_cards_per_suit=13).diamonds
... )
>>> wkflw = construct(red_total, simplify_ids=True)
>>> cwl = render(wkflw)["__root__"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs:
  shuffle-1-max_cards_per_suit:
    default: 13
    label: max_cards_per_suit
    type: int
outputs:
  out:
    label: out
    outputSource: sum-1/out
    type: int
steps:
  shuffle-1:
    in:
      max_cards_per_suit:
        source: shuffle-1-max_cards_per_suit
    out:
      clubs:
        label: clubs
        type: int
      diamonds:
        label: diamonds
        type: int
      hearts:
        label: hearts
        type: int
      spades:
        label: spades
        type: int
    run: shuffle
  sum-1:
    in:
      left:
        source: shuffle-1/hearts
      right:
        source: shuffle-1/diamonds
    out:
    - out
    run: sum

```

## Chaining workflows together

As `@workflow()`s are essentially syntactic sugar for `@task()`s they can be chained together.

* **Question**: is the output different from the case when they are `@task()`?


<!-- A special form of nested task is available to help divide up
more complex workflows: the *subworkflow*. By wrapping logic in subflows,
dewret will produce multiple output workflows that reference each other. -->

```python
>>> import sys
>>> import yaml
>>> from attrs import define
>>> from numpy import random
>>> from dewret.tasks import task, construct, workflow
>>> from dewret.renderers.cwl import render
>>> @define
... class PackResult:
...     hearts: int
...     clubs: int
...     spades: int
...     diamonds: int
>>>
>>> @task()
... def sum(left: int, right: int) -> int:
...    return left + right
>>>
>>> @task()
... def shuffle(max_cards_per_suit: int) -> PackResult:
...    """Fill a random pile from a card deck, suit by suit."""
...    return PackResult(
...        hearts=random.randint(max_cards_per_suit),
...        clubs=random.randint(max_cards_per_suit),
...        spades=random.randint(max_cards_per_suit),
...        diamonds=random.randint(max_cards_per_suit)
...    )
>>> @workflow()
... def red_total() -> int:
...     return sum(
...         left=shuffle(max_cards_per_suit=13).hearts,
...         right=shuffle(max_cards_per_suit=13).diamonds
...     )
>>> @workflow()
... def black_total() -> int:
...     return sum(
...         left=shuffle(max_cards_per_suit=13).spades,
...         right=shuffle(max_cards_per_suit=13).clubs
...     )
>>> total = sum(left=red_total(), right=black_total())
>>> wkflw = construct(total, simplify_ids=True)
>>> cwl = render(wkflw)["__root__"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs: {}
outputs:
  out:
    label: out
    outputSource: sum-1/out
    type: int
steps:
  black_total-1:
    in: {}
    out:
    - out
    run: black_total
  red_total-1:
    in: {}
    out:
    - out
    run: red_total
  sum-1:
    in:
      left:
        source: red_total-1/out
      right:
        source: black_total-1/out
    out:
    - out
    run: sum

```

As we have used subworkflow to wrap the colour totals, the outer workflow
contains references to them only. The subworkflows are now returned by `render`
as a second term.

```python
>>> import sys
>>> import yaml
>>> from attrs import define
>>> from numpy import random
>>> from dewret.tasks import task, construct, workflow
>>> from dewret.renderers.cwl import render
>>> @define
... class PackResult:
...     hearts: int
...     clubs: int
...     spades: int
...     diamonds: int
>>>
>>> @task()
... def shuffle(max_cards_per_suit: int) -> PackResult:
...    """Fill a random pile from a card deck, suit by suit."""
...    return PackResult(
...        hearts=random.randint(max_cards_per_suit),
...        clubs=random.randint(max_cards_per_suit),
...        spades=random.randint(max_cards_per_suit),
...        diamonds=random.randint(max_cards_per_suit)
...    )
>>> @task()
... def sum(left: int, right: int) -> int:
...    return left + right
>>>
>>> @workflow()
... def red_total() -> int:
...     return sum(
...         left=shuffle(max_cards_per_suit=13).hearts,
...         right=shuffle(max_cards_per_suit=13).diamonds
...     )
>>> @workflow()
... def black_total() -> int:
...     return sum(
...         left=shuffle(max_cards_per_suit=13).spades,
...         right=shuffle(max_cards_per_suit=13).clubs
...     )
>>> total = sum(left=red_total(), right=black_total())
>>> wkflw = construct(total, simplify_ids=True)
>>> cwl = render(wkflw)
>>> yaml.dump(cwl["red_total-1"], sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs: {}
outputs:
  out:
    label: out
    outputSource: sum-1-1/out
    type: int
steps:
  shuffle-1-1:
    in:
      max_cards_per_suit:
        default: 13
    out:
      clubs:
        label: clubs
        type: int
      diamonds:
        label: diamonds
        type: int
      hearts:
        label: hearts
        type: int
      spades:
        label: spades
        type: int
    run: shuffle
  sum-1-1:
    in:
      left:
        source: shuffle-1-1/hearts
      right:
        source: shuffle-1-1/diamonds
    out:
    - out
    run: sum

```

## Complex input types and factories 

Sometimes we want to take complex Python input, not just basic types.
Not all serialization support this, but the `factory` function lets us
wrap a simple call, usually a constructor, that takes _only_ raw arguments.
This can then rendered as either a step or a parameter depending on whether
the chosen renderer has the capability.

### Input factories as task

Below is the default output, treating `Pack` as a task.

```python
>>> import sys
>>> import yaml
>>> from dewret.tasks import workflow, factory, workflow, construct, task
>>> from attrs import define
>>> from dewret.renderers.cwl import render
>>> @define
... class PackResult:
...     hearts: int
...     clubs: int
...     spades: int
...     diamonds: int
>>>
>>> Pack = factory(PackResult)
>>>
>>> @task()
... def sum(left: int, right: int) -> int:
...    return left + right
>>>
>>> @workflow()
... def black_total(pack: PackResult) -> int:
...     return sum(
...         left=pack.spades,
...         right=pack.clubs
...     )
>>> pack = Pack(hearts=13, spades=13, diamonds=13, clubs=13)
>>> wkflw = construct(black_total(pack=pack), simplify_ids=True)
>>> cwl = render(wkflw)["__root__"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs:
  PackResult-1-clubs:
    default: 13
    label: clubs
    type: int
  PackResult-1-diamonds:
    default: 13
    label: diamonds
    type: int
  PackResult-1-hearts:
    default: 13
    label: hearts
    type: int
  PackResult-1-spades:
    default: 13
    label: spades
    type: int
outputs:
  out:
    label: out
    outputSource: black_total-1/out
    type: int
steps:
  PackResult-1:
    in:
      clubs:
        source: PackResult-1-clubs
      diamonds:
        source: PackResult-1-diamonds
      hearts:
        source: PackResult-1-hearts
      spades:
        source: PackResult-1-spades
    out:
      clubs:
        label: clubs
        type: int
      diamonds:
        label: diamonds
        type: int
      hearts:
        label: hearts
        type: int
      spades:
        label: spades
        type: int
    run: PackResult
  black_total-1:
    in:
      pack:
        source: PackResult-1/out
    out:
    - out
    run: black_total

```

### Input factories as a parameter

The CWL renderer is also able to treat `pack` as a parameter, if complex
types are allowed.

```python
>>> import sys
>>> import yaml
>>> from dewret.tasks import task, factory, workflow, construct
>>> from attrs import define
>>> from dewret.renderers.cwl import render
>>> @define
... class PackResult:
...     hearts: int
...     clubs: int
...     spades: int
...     diamonds: int
>>>
>>> Pack = factory(PackResult)
>>> @task()
... def sum(left: int, right: int) -> int:
...    return left + right
>>>
>>> @workflow()
... def black_total(pack: PackResult) -> int:
...     return sum(
...         left=pack.spades,
...         right=pack.clubs
...     )
>>> pack = Pack(hearts=13, spades=13, diamonds=13, clubs=13)
>>> wkflw = construct(black_total(pack=pack), simplify_ids=True)
>>> cwl = render(wkflw, allow_complex_types=True, factories_as_params=True)["black_total-1"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: v1.2
inputs:
  pack:
    label: pack
    type: record
outputs:
  out:
    label: out
    outputSource: sum-1-1/out
    type: int
steps:
  sum-1-1:
    in:
      left:
        source: pack/spades
      right:
        source: pack/clubs
    out:
    - out
    run: sum

```

