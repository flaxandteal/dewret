# Quickstart

## Introduction 

### Description 

Dewret is a tool designed for creating complex workflows, written in a dynamic style, to be rendered to a static representation. Dewret provides a programmatic python interface to multiple declarative workflow engines, where workflows are often written in a yaml-like syntax. It makes it easier for users to define tasks and organize them into workflows. Currently, Dewret supports two renderers: Snakemake and CWL, which generate yamls in the corresponding workflow languages.

### What are Workflows?

Workflows are a collection of tasks or steps designed to automate complex processes. These processes are common in fields like data science, scientific computing and software development, where you can ensure automation. Traditionally, managing workflows can be challenging due to the diversity of backend systems and the complexity of configurations involved.

### What Makes Dewret Unique? Why should I use Dewret?

Dewret stands out by providing a unified and simplified interface for workflow management, making it accessible to users with varying levels of experience. Here are some key features that make Dewret unique:

- **Consistency**: offers a consistent interface for defining tasks and workflows.
- **Optimization**: creating a declarative workflow opens up possibilities for static analysis and refactoring before execution.
- **Customization**: dewret offers the ability to create custom renderers for workflows in desired languages. This includes default support for CWL and Snakemake workflow languages. The capability to render a single workflow into multiple declarative languages enables users to experiment with different workflow engines.
- **Git-versionable workflows**: while code can be versioned, changes in a dynamic workflow may not clearly correspond to changes in the executed workflow. By defining a static workflow that is rendered from the dynamic or programmatic workflow, we maintain a precise and trackable history.
- **Default Renderers**: Snakemake and CWL.
- **Debugging**: a number of classes of workflow planning bugs will not appear until late in a simulation run that might take days or weeks. Having a declarative and static workflow definition document post-render provides enhanced possibilities for static analysis, helping to catch these issues before startup.
- **Continuous Integration and Testing**: complex dynamic workflows can be rapidly sense-checked in CI without needing all the hardware and internal algorithms present to run them.

## Installation for pure users

If you simply want to use Dewret to run workflows, you can install it from PyPI or Conda.

### From PyPI:
```shell
pip install dewret
```

### From Conda:
```shell
conda install conda-forge::dewret
```

## Installation for developers

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
>>> cwl = render(workflow)["__root__"]
>>> yaml.dump(cwl, sys.stdout, indent=2)
class: Workflow
cwlVersion: 1.2
inputs:
  increment-1-num:
    default: 3
    label: num
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
