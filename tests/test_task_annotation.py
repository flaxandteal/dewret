"""Verify that the task decorator works with TypedDict and NotRequired fields."""

import pytest
import yaml
from typing import TypedDict, NotRequired, Unpack, Any
from dewret.tasks import task, construct, TaskException
from dewret.renderers.cwl import render


class TaskConfig(TypedDict):
    """TypedDict for testing dewret @task() arguement validation."""

    foo: int
    bar: int
    baz: int
    qux: NotRequired[float]


@task()
def increment(**config: Unpack[TaskConfig]) -> dict[str, Any]:
    """A simple task that unpacks arguments to a dictionary of parameters."""
    return {**config}


def test_valid_input() -> None:
    """Test that the task decorator works with TypedDict and NotRequired fields."""
    result = increment(foo=3, bar=4, baz=5, qux=4.0)
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]
    assert rendered == yaml.safe_load("""
class: Workflow
cwlVersion: 1.2
inputs:
  increment-1-qux:
    default: 4.0
    label: qux
    type: float
  increment-1-foo:
    default: 3
    label: foo
    type: int
  increment-1-bar:
    default: 4
    label: bar
    type: int
  increment-1-baz:
    default: 5
    label: baz
    type: int
outputs:
  out:
    label: out
    outputSource: increment-1/out
    type: record
steps:
  increment-1:
    in:
      qux:
        source: increment-1-qux
      foo:
        source: increment-1-foo
      bar:
        source: increment-1-bar
      baz:
        source: increment-1-baz
    out:
    - out
    run: increment
        """)


def test_optional_field_missing() -> None:
    """Test that the task decorator works with optional fields missing."""
    result = increment(foo=3, bar=4, baz=5)
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]
    assert rendered == yaml.safe_load("""
class: Workflow
cwlVersion: 1.2
inputs:
  increment-1-foo:
    default: 3
    label: foo
    type: int
  increment-1-bar:
    default: 4
    label: bar
    type: int
  increment-1-baz:
    default: 5
    label: baz
    type: int
outputs:
  out:
    label: out
    outputSource: increment-1/out
    type: record
steps:
  increment-1:
    in:
      foo:
        source: increment-1-foo
      bar:
        source: increment-1-bar
      baz:
        source: increment-1-baz
    out:
    - out
    run: increment
        """)


def test_required_field_missing() -> None:
    """Test that the task decorator throws an exception if a required field is missing."""
    with pytest.raises(TaskException, match="missing required keyword argument"):
        increment(foo=1, qux=4.0)  # type: ignore


def test_invalid_input_for_required_field() -> None:
    """Test that the task decorator throws an exception if a required field is invalid."""
    with pytest.raises(TaskException, match="got invalid type for argument "):
        increment(foo=1, bar="string", baz=4)  # type: ignore


def test_invalid_input_for_NotRequired_field() -> None:
    """Test that the task decorator throws an exception if a optional field is invalid."""
    with pytest.raises(TaskException, match="got invalid type for argument "):
        increment(foo=1, bar=3, baz=4, qux=4)
