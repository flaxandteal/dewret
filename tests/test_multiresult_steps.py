"""Verify CWL can be made with split up and nested calls."""

import yaml
from attr import define
from dataclasses import dataclass
from typing import Iterable
from dewret.tasks import task, construct, workflow
from dewret.core import set_configuration
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


@task()
def list_cast(iterable: Iterable[float]) -> list[float]:
    """Converts an iterable structure with float elements into a list of floats."""
    return list(iterable)


@task()
def pair(left: int, right: float) -> tuple[int, float]:
    """Pairs two values."""
    return (left, right)


@workflow()
def algorithm() -> float:
    """Sum two split values."""
    return combine(left=split().first, right=split().second)


@workflow()
def algorithm_with_pair() -> tuple[int, float]:
    """Pairs two split dataclass values."""
    return pair(left=split_into_dataclass().first, right=split_into_dataclass().second)


@workflow()
def algorithm_with_dataclasses() -> float:
    """Sums two split dataclass values."""
    return combine(
        left=split_into_dataclass().first, right=split_into_dataclass().second
    )


@task()
def split() -> SplitResult:
    """Create a split result with two fields."""
    return SplitResult(first=1, second=2)


@task()
def split_into_dataclass() -> SplitResultDataclass:
    """Create a result with two fields."""
    return SplitResultDataclass(first=1, second=2)


def test_subworkflow() -> None:
    """Check whether we can link between multiple steps and have parameters.

    Produces CWL that has references between multiple steps.
    """
    workflow = construct(split(), simplify_ids=True)
    rendered = render(workflow)["__root__"]

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
                    type: float
        steps:
          split-1:
            in: {}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: float
            run: split
    """)


def test_field_of_subworkflow() -> None:
    """Tests whether a directly-output nested task can have fields."""
    workflow = construct(split().first, simplify_ids=True)
    rendered = render(workflow)["__root__"]

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
                type: float
            run: split
    """)


def test_field_of_subworkflow_into_dataclasses() -> None:
    """Tests whether a directly-output nested task can have fields."""
    workflow = construct(split_into_dataclass().first, simplify_ids=True)
    rendered = render(workflow)["__root__"]

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
                type: float
            run: split_into_dataclass
    """)


def test_complex_field_of_subworkflow() -> None:
    """Tests whether a task can sum complex structures."""
    with set_configuration(flatten_all_nested=True):
        workflow = construct(algorithm(), simplify_ids=True)
        rendered = render(workflow)["__root__"]

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          out:
            label: out
            outputSource: combine-1/out
            type: float
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
                type: float
            run: split
    """)


def test_complex_field_of_subworkflow_with_dataclasses() -> None:
    """Tests whether a task can insert result fields into other steps."""
    with set_configuration(flatten_all_nested=True):
        result = algorithm_with_dataclasses()
        workflow = construct(result, simplify_ids=True)
        rendered = render(workflow)["__root__"]

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          out:
            label: out
            outputSource: combine-1/out
            type: float
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
                type: float
            run: split_into_dataclass
    """)


def test_pair_can_be_returned_from_step() -> None:
    """Tests whether a task can insert result fields into other steps."""
    with set_configuration(flatten_all_nested=True):
        workflow = construct(algorithm_with_pair(), simplify_ids=True)
        rendered = render(workflow)["__root__"]

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          out:
            label: out
            outputSource: pair-1/out
            items:
              - int
              - float
            type: array
        steps:
          pair-1:
            in:
                left:
                    source: split_into_dataclass-1/first
                right:
                    source: split_into_dataclass-1/second
            out: [out]
            run: pair
          split_into_dataclass-1:
            in: {}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: float 
            run: split_into_dataclass
    """)


def test_list_can_be_returned_from_step() -> None:
    """Tests whether a task can insert result fields into other steps."""
    with set_configuration(flatten_all_nested=True):
        workflow = construct(
            list_cast(iterable=algorithm_with_pair()), simplify_ids=True
        )
        rendered = render(workflow)["__root__"]

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          out:
            label: out
            outputSource: list_cast-1/out
            items: float
            type: array
        steps:
          list_cast-1:
            in:
                iterable:
                    source: pair-1/out
            out: [out]
            run: list_cast
          pair-1:
            in:
                left:
                    source: split_into_dataclass-1/first
                right:
                    source: split_into_dataclass-1/second
            out: [out]
            run: pair
          split_into_dataclass-1:
            in: {}
            out:
              first:
                label: first
                type: int
              second:
                label: second
                type: float
            run: split_into_dataclass
    """)
