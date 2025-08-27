# Renderers

_Renderers_ are a function that takes a [task](https://flaxandteal.github.io/dewret/reference/dewret/tasks/#task), which can
be assumed to have a `__workflow__` member of type [Workflow](https://flaxandteal.github.io/dewret/reference/dewret/workflow/#workflow), and return
a YAML-serializable nested `dict` structure.


## CWL

The default renderer is for the Common Workflow Language. It implements a very small subset
of functionality, and is not yet strictly standards compliant. It assumes that all `run`
names can be interpreted in the context of the workflow module's global scope.

An additional arg can be passed to the CWL renderer to allow the steps to be ordered to match the execution within the python script

```
workflow = construct(example_workflow(), simplify_ids=True)

rendered = render(workflow, sort_steps=True)
```

If the tasks within the script are listed as:
```
@workflow()
def example_workflow() -> int:
    """Test workflow with multiple tasks"""
    step1 = increment(num=1)
    step2 = increment(num=5)
    step3 = sum(left=step1, right=step2)
    return step3
```
Then the yaml will be returned as:
```
{
    "cwlVersion": 1.2,
    "class": "Workflow",
    "inputs": {},
    "outputs":
        { "out": { "label": "out", "type": "int", "outputSource": "sum-1-1/out" } },
    "steps":
        {
        "increment-1-2":
            {
            "run": "increment",
            "in": { "num": { "default": 1 } },
            "out": ["out"],
            },
        "increment-1-1":
            {
            "run": "increment",
            "in": { "num": { "default": 5 } },
            "out": ["out"],
            },
        "sum-1-1":
            {
            "run": "sum",
            "in":
                {
                "left": { "source": "increment-1-2/out" },
                "right": { "source": "increment-1-1/out" },
                },
            "out": ["out"],
            },
        },
}
```

The sort-steps can also be used in the CLI passing it as a flag:

```
python -m dewret workflows.py example_workflow --sort-steps
```

## Custom

...
