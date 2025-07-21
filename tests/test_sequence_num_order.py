"""Check sequential numbers are added in task order"""

import pytest
import yaml
from dewret.tasks import construct, task, workflow
from dewret.renderers.cwl import render
from dewret.workflow import NestedStep

@task()
def increment(num: int) -> int:
    """Simple task to increment a number"""
    return num + 1

@task()
def sum(left: int, right:int) -> int:
    """A task to add two numbers"""
    return left + right

@workflow()
def example_workflow() -> int:
    """Test workflow with multiple tasks"""
    step1 = increment(num=1)
    step2 = increment(num=5)
    step3 = sum(left=step1, right=step2)
    return step3

@workflow()
def linear_workflow() -> int:
    step1 = increment(num=1)
    step2 = increment(num=step1)
    step3 = increment(num=step2)
    return step3

def test_sequence_number_in_example_workflow():
    """Test to check if sequence numbers are correctly added in the order the tasks are added"""
    from dewret.tasks import workflow

    workflow = construct(example_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep), "Expected a NestedStep"

    steps = list(nested_step.subworkflow.steps)

    assert len(steps) == 3, f"Expected 3 steps got {len(steps)}"

    for i, step in enumerate(steps):
        print(f"Sequence num {step.__sequence_num__} is set at position {i} in the list")
        assert step.__sequence_num__ == i, f"Step seq num {step.__sequence_num__} is {i} in the list"

def test_sequence_number_in_linear_workflow():
    """Test to check if sequence numbers are correctly added in the order the tasks are added"""
    from dewret.tasks import workflow

    workflow = construct(linear_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep), "Expected a NestedStep"

    steps = list(nested_step.subworkflow.steps)

    assert len(steps) == 3, f"Expected 3 steps got {len(steps)}"

    for i, step in enumerate(steps):
        print(f"Sequence num {step.__sequence_num__} is set at position {i} in the list")
        assert step.__sequence_num__ == i, f"Step seq num {step.__sequence_num__} is {i} in the list"

def test_render_outputs_list_in_order():
    """Test to see if the yaml renders with the list in the correct order"""

    workflow = construct(example_workflow(), simplify_ids=True)

    rendered = render(workflow)["example_workflow-1"]

    assert rendered == yaml.safe_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            { "out": { "label": "out", "type": "int", "outputSource": "increment-1-3/out" } },
        "steps":
            {
            "increment-1-1":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 2 } },
                "out": ["out"],
                },
            "sum-1-1":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "increment-1-1/out" },
                    "right": { "source": "increment-1-2/out" },
                    },
                "out": ["out"],
                },
            },
        }
    """)

def test_render_linear_outputs_list_in_order():
    """Test to see if the yaml renders the linear task list in the correct order"""

    workflow = construct(linear_workflow(), simplify_ids=True)

    rendered = render(workflow)["linear_workflow-1"]

    assert rendered == yaml.safe_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                { "label": "out", "type": "int", "outputSource": "increment-1-3/out" },
            },
        "steps":
            {
            "increment-1-1":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-2":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1/out" } },
                "out": ["out"],
                },
            "increment-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-2/out" } },
                "out": ["out"],
                },
            },
        }
    """)                                      
    

