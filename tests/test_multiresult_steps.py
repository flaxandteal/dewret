"""Verify CWL can be made with split up and nested calls."""

import yaml
from attr import define
from dewret.tasks import task, construct, nested_task
from dewret.renderers.cwl import render
from dewret.workflow import Lazy
from ._lib.extra import double, mod10, sum, increase

STARTING_NUMBER: int = 23

@define
class SplitResult:
    """Test class showing two named values."""
    first: int
    second: float

@task()
def combine(left: int, right: float) -> float:
    """Sum two values."""
    return left + right

@nested_task()
def algorithm() -> float:
    """Sum two split values."""
    return combine(left=split().first, right=split().second)

@task()
def split() -> SplitResult:
    """Create a result with two fields."""
    return SplitResult(first=1, second=2)

def test_nested_task() -> None:
    """Check whether we can link between multiple steps and have parameters.

    Produces CWL that has references between multiple steps.
    """
    workflow = construct(split(), simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load(f"""
        class: Workflow
        cwlVersion: 1.2
        inputs: {{}}
        outputs:
          out:
            label: out
            outputSource: split-1/out
            type: record
            fields:
                first:
                    label: first
                    type: int
                second:
                    label: second
                    type: double
        steps:
          split-1:
            in: {{}}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: double
            run: split
    """)

def test_field_of_nested_task() -> None:
    """Tests whether a directly-output nested task can have fields."""
    workflow = construct(split().first, simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load(f"""
        class: Workflow
        cwlVersion: 1.2
        inputs: {{}}
        outputs:
          first:
            label: first
            outputSource: split-1/first
            type: int
        steps:
          split-1:
            in: {{}}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: double
            run: split
    """)

def test_complex_field_of_nested_task() -> None:
    """Tests whether a task can insert result fields into other steps."""
    workflow = construct(algorithm(), simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load(f"""
        class: Workflow
        cwlVersion: 1.2
        inputs: {{}}
        outputs:
          out:
            label: out
            outputSource: combine-1/out
            type: double
        steps:
          combine-1:
            in:
                left:
                    source: split-1/first
                right:
                    source: split-1/second
            out: [out]
            run: combine
          split-1:
            in: {{}}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: double
            run: split
    """)
