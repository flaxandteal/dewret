"""Verify CWL can be made with split up and nested calls."""

import yaml
from attr import define
from dataclasses import dataclass
from dewret.tasks import task, construct, nested_task
from dewret.renderers.cwl import render

STARTING_NUMBER: int = 23


@define
class SplitResult:
    """Test class showing two named values, using attrs."""

    first: int
    second: float


@dataclass
class SplitResultDataclass:
    """Test class showing two named values, using dataclasses."""

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


@nested_task()
def algorithm_with_dataclasses() -> float:
    """Sum two split values."""
    return combine(
        left=split_into_dataclass().first, right=split_into_dataclass().second
    )


@task()
def split() -> SplitResult:
    """Create a result with two fields."""
    return SplitResult(first=1, second=2)


@task()
def split_into_dataclass() -> SplitResultDataclass:
    """Create a result with two fields."""
    return SplitResultDataclass(first=1, second=2)


def test_nested_task() -> None:
    """Check whether we can link between multiple steps and have parameters.

    Produces CWL that has references between multiple steps.
    """
    workflow = construct(split(), simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
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
            in: {}
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

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          first:
            label: first
            outputSource: split-1/first
            type: int
        steps:
          split-1:
            in: {}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: double
            run: split
    """)


def test_field_of_nested_task_into_dataclasses() -> None:
    """Tests whether a directly-output nested task can have fields."""
    workflow = construct(split_into_dataclass().first, simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          first:
            label: first
            outputSource: split_into_dataclass-1/first
            type: int
        steps:
          split_into_dataclass-1:
            in: {}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: double
            run: split_into_dataclass
    """)


def test_complex_field_of_nested_task() -> None:
    """Tests whether a task can insert result fields into other steps."""
    workflow = construct(algorithm(), simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
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
            in: {}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: double
            run: split
    """)


def test_complex_field_of_nested_task_with_dataclasses() -> None:
    """Tests whether a task can insert result fields into other steps."""
    result = algorithm_with_dataclasses()
    workflow = construct(result, simplify_ids=True, nested=False)
    rendered = render(workflow)

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          out:
            label: out
            outputSource: combine-1/out
            type: double
        steps:
          combine-1:
            in:
                left:
                    source: split_into_dataclass-1/first
                right:
                    source: split_into_dataclass-1/second
            out: [out]
            run: combine
          split_into_dataclass-1:
            in: {}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: double
            run: split_into_dataclass
    """)
