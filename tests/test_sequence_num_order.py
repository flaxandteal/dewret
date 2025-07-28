"""Check sequential numbers are added in task order"""

import yaml
from dewret.tasks import construct, task, workflow
from dewret.renderers.cwl import render
from dewret.workflow import NestedStep, BaseStep


@task()
def increment(num: int) -> int:
    """Simple task to increment a number"""
    return num + 1


@task()
def sum(left: int, right: int) -> int:
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


@workflow()
def long_workflow() -> int:
    """A longer workflow with several steps to test ordering"""
    step1 = increment(num=1)
    step2 = increment(num=step1)
    step3 = increment(num=step2)
    step4 = sum(left=step3, right=step2)
    step5 = sum(left=4, right=6)
    step6 = sum(left=step4, right=step5)
    step7 = increment(num=step6)
    step8 = increment(num=step7)
    return step8


def output():
    """A function to run several tasks without a workflow"""
    step1 = increment(num=1)
    step2 = increment(num=5)
    step3 = increment(num=7)
    step4 = sum(left=step1, right=step2)
    step5 = sum(left=step4, right=step3)
    return step5


@workflow()
def combined_workflow() -> int:
    step1 = linear_workflow()
    step2 = long_workflow()
    step3 = sum(left=step1, right=step2)
    return step3


def check_sequence_numbers_in_sequence(
    sequenced_steps: dict[str, BaseStep],
) -> tuple[bool, list]:
    """Check if sequence numbers are not None and are incrementing correctly."""
    steps = list(sequenced_steps.values())

    for i in range(len(steps) - 1):
        current_num = steps[i].__sequence_num__
        next_num = steps[i + 1].__sequence_num__

        # Ensure both are not None before comparison
        if current_num is None or next_num is None:
            return (False, steps)
        if current_num >= next_num:
            return (False, steps)

    return (True, steps)


def test_sequence_numbers_are_sequential_in_example_workflow():
    """Test to check if the sequence numbers are correctly ordered in the example workflow"""
    workflow = construct(example_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep)

    sequenced_steps = nested_step.subworkflow.sequenced_steps

    assert (
        len(sequenced_steps) == 3
    ), f"Expected 3 steps received {len(sequenced_steps)}"

    in_sequence = check_sequence_numbers_in_sequence(sequenced_steps)

    assert (
        in_sequence[0] == True
    ), f"The step sequence numbers did not iterate sequentially {in_sequence[1]}"


def test_sequence_number_in_linear_workflow():
    """Test to check if sequence numbers are correctly ordered in the linear workflow"""
    workflow = construct(linear_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep)

    sequenced_steps = nested_step.subworkflow.sequenced_steps

    assert (
        len(sequenced_steps) == 3
    ), f"Expected 3 steps received {len(sequenced_steps)}"

    in_sequence = check_sequence_numbers_in_sequence(sequenced_steps)

    assert (
        in_sequence[0] == True
    ), f"The step sequence numbers did not iterate sequentially {in_sequence[1]}"


def test_sequence_number_in_long_workflow():
    """Test to check if sequence numbers are correctly ordered in the long workflow"""
    workflow = construct(long_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep)

    sequenced_steps = nested_step.subworkflow.sequenced_steps

    assert (
        len(sequenced_steps) == 8
    ), f"Expected 8 steps received {len(sequenced_steps)}"

    in_sequence = check_sequence_numbers_in_sequence(sequenced_steps)

    assert (
        in_sequence[0] == True
    ), f"The step sequence numbers did not iterate sequentially {in_sequence[1]}"


def test_sequence_number_in_combined_workflow():
    """Test to check if sequence numbers are correctly ordered in the long workflow"""
    workflow = construct(combined_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep)

    sequenced_steps = nested_step.subworkflow.sequenced_steps

    assert (
        len(sequenced_steps) == 3
    ), f"Expected 3 steps received {len(sequenced_steps)}"

    in_sequence = check_sequence_numbers_in_sequence(sequenced_steps)

    assert (
        in_sequence[0] == True
    ), f"The step sequence numbers did not iterate sequentially {in_sequence[1]}"


def test_render_outputs_list_in_order_for_example_workflow():
    """Test to see if the yaml renders with the list in the correct order"""
    workflow = construct(example_workflow(), simplify_ids=True)

    rendered = render(workflow, sort_steps=True)["example_workflow-1"]

    assert rendered == yaml.safe_load("""
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
    """)


def test_render_linear_outputs_list_in_order():
    """Test to see if the yaml renders the linear task list in the correct order"""
    workflow = construct(linear_workflow(), simplify_ids=True)

    rendered = render(workflow, sort_steps=True)["linear_workflow-1"]

    assert rendered == yaml.safe_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                { "label": "out", "type": "int", "outputSource": "increment-1-1/out" },
            },
        "steps":
            {
            "increment-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-2/out" } },
                "out": ["out"],
                },
            "increment-1-1":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-3/out" } },
                "out": ["out"],
                },
            },
        }
    """)


def test_render_long_workflow_outputs_list_in_order():
    """Test to see if the yaml renders the linear task list in the correct order"""
    workflow = construct(long_workflow(), simplify_ids=True)

    rendered = render(workflow, sort_steps=True)["long_workflow-1"]

    assert rendered == yaml.safe_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                { "label": "out", "type": "int", "outputSource": "increment-1-5/out" },
            },
        "steps":
            {
            "increment-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-2/out" } },
                "out": ["out"],
                },
            "increment-1-1":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-3/out" } },
                "out": ["out"],
                },
            "sum-1-3":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "increment-1-1/out" },
                    "right": { "source": "increment-1-3/out" },
                    },
                "out": ["out"],
                },
            "sum-1-2":
                {
                "run": "sum",
                "in": { "left": { "default": 4 }, "right": { "default": 6 } },
                "out": ["out"],
                },
            "sum-1-1":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "sum-1-3/out" },
                    "right": { "source": "sum-1-2/out" },
                    },
                "out": ["out"],
                },
            "increment-1-4":
                {
                "run": "increment",
                "in": { "num": { "source": "sum-1-1/out" } },
                "out": ["out"],
                },
            "increment-1-5":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-4/out" } },
                "out": ["out"],
                },
            },
        }
    """)


def test_render_nested_workflow_outputs_list_in_order():
    """Test to see if the yaml renders the linear task list in the correct order"""
    workflow = construct(combined_workflow(), simplify_ids=True)

    rendered_combined = render(workflow, sort_steps=True)["combined_workflow-1"]
    rendered_linear = render(workflow, sort_steps=True)["linear_workflow-1-1"]
    rendered_long = render(workflow, sort_steps=True)["long_workflow-1-1"]

    assert rendered_combined == yaml.safe_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            { "out": { "label": "out", "type": "int", "outputSource": "sum-1-1/out" } },
        "steps":
            {
            "linear_workflow-1-1":
                { "run": "linear_workflow", "in": {}, "out": ["out"] },
            "long_workflow-1-1": { "run": "long_workflow", "in": {}, "out": ["out"] },
            "sum-1-1":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "linear_workflow-1-1/out" },
                    "right": { "source": "long_workflow-1-1/out" },
                    },
                "out": ["out"],
                },
            },
        }
    """)

    assert rendered_linear == yaml.safe_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                {
                "label": "out",
                "type": "int",
                "outputSource": "increment-1-1-1/out",
                },
            },
        "steps":
            {
            "increment-1-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-2/out" } },
                "out": ["out"],
                },
            "increment-1-1-1":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-3/out" } },
                "out": ["out"],
                },
            },
        }
    """)

    assert rendered_long == yaml.safe_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                {
                "label": "out",
                "type": "int",
                "outputSource": "increment-1-1-5/out",
                },
            },
        "steps":
            {
            "increment-1-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-2/out" } },
                "out": ["out"],
                },
            "increment-1-1-1":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-3/out" } },
                "out": ["out"],
                },
            "sum-1-1-3":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "increment-1-1-1/out" },
                    "right": { "source": "increment-1-1-3/out" },
                    },
                "out": ["out"],
                },
            "sum-1-1-2":
                {
                "run": "sum",
                "in": { "left": { "default": 4 }, "right": { "default": 6 } },
                "out": ["out"],
                },
            "sum-1-1-1":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "sum-1-1-3/out" },
                    "right": { "source": "sum-1-1-2/out" },
                    },
                "out": ["out"],
                },
            "increment-1-1-4":
                {
                "run": "increment",
                "in": { "num": { "source": "sum-1-1-1/out" } },
                "out": ["out"],
                },
            "increment-1-1-5":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-4/out" } },
                "out": ["out"],
                },
            },
        }
    """)
