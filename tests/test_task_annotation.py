"""Verify that the task decorator works with TypedDict and NotRequired fields."""

import pytest
import yaml
from dewret.core import set_configuration
from typing import TypedDict, NotRequired, Unpack, Any, Union, Literal, Optional
from dewret.tasks import task, construct, TaskException
from dewret.renderers.cwl import render


class TaskConfig(TypedDict):
    """TypedDict for testing dewret @task() argument validation."""

    foo: int
    bar: int
    baz: Optional[int]  # Optional fields are required
    qux: NotRequired[float]


@task()
def increment(**config: Unpack[TaskConfig]) -> dict[str, Any]:
    """A simple task that unpacks arguments to a dictionary of parameters."""
    return {**config}


def test_valid_input() -> None:
    """Test that all fields, including NotRequired and Optional, are passed correctly."""
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


def test_not_required_field_missing() -> None:
    """Test that omitting a NotRequired field does not raise an error."""
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
    """Test that omitting a required field raises a TaskException."""
    with pytest.raises(TaskException, match="missing required keyword argument"):
        increment(foo=1, baz=4, qux=4.0)  # type: ignore


def test_invalid_input_for_required_field() -> None:
    """Test that an invalid type for a required field raises a TaskException."""
    with pytest.raises(TaskException, match="got invalid type for argument "):
        increment(foo=1, bar="string", baz=4)  # type: ignore


def test_invalid_input_for_NotRequired_field() -> None:
    """Test that an invalid type for a NotRequired field raises a TaskException."""
    with pytest.raises(TaskException, match="got invalid type for argument "):
        increment(foo=1, bar=3, baz=4, qux=4)


def test_optional_field_missing() -> None:
    """Test that missing an Optional field with no default raises a TaskException."""
    with pytest.raises(
        TaskException,
        match="missing a value for keyword argument: 'baz' 'baz' is an Optional argument, but no default value provided.",
    ):
        increment(foo=1, bar=3, qux=4.1)  # type: ignore


# --- Literal ---
class LiteralConfig(TypedDict):
    """TypedDict for testing dewret @task() with Literal values."""

    baz: Literal["fast", "slow"]
    qux: Literal["fast", "medium", "slow"]


@task()
def literal_task(foo: float, bar: int, **config: Unpack[LiteralConfig]) -> str:
    """Task accepting Literal values as part of config."""
    return config["baz"]


def test_literal_valid() -> None:
    """Test that valid Literal values pass correctly."""
    result = literal_task(foo=3.14, bar=1, baz="fast", qux="medium")
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]
    assert rendered == yaml.safe_load("""
class: Workflow
cwlVersion: 1.2
inputs:
  literal_task-1-baz:
    default: fast
    label: baz
    type: string
  literal_task-1-qux:
    default: medium
    label: qux
    type: string
  literal_task-1-foo:
    default: 3.14
    label: foo
    type: float
  literal_task-1-bar:
    default: 1
    label: bar
    type: int
outputs:
  out:
    label: out
    outputSource: literal_task-1/out
    type: string
steps:
  literal_task-1:
    in:
      baz:
        source: literal_task-1-baz
      qux:
        source: literal_task-1-qux
      foo:
        source: literal_task-1-foo
      bar:
        source: literal_task-1-bar
    out:
      - out
    run: literal_task
        """)


def test_literal_valid_with_positional_args_true() -> None:
    """Test Literal support with positional arguments enabled."""
    with set_configuration(allow_positional_args=True):
        result = literal_task(3.14, 1, "fast", "medium")  # type: ignore
        workflow = construct(result, simplify_ids=True)
        rendered = render(workflow)["__root__"]
        assert rendered == yaml.safe_load("""
class: Workflow
cwlVersion: 1.2
inputs:
  literal_task-1-baz:
    default: fast
    label: baz
    type: string
  literal_task-1-qux:
    default: medium
    label: qux
    type: string
  literal_task-1-foo:
    default: 3.14
    label: foo
    type: float
  literal_task-1-bar:
    default: 1
    label: bar
    type: int
outputs:
  out:
    label: out
    outputSource: literal_task-1/out
    type: string
steps:
  literal_task-1:
    in:
      baz:
        source: literal_task-1-baz
      qux:
        source: literal_task-1-qux
      foo:
        source: literal_task-1-foo
      bar:
        source: literal_task-1-bar
    out:
      - out
    run: literal_task
        """)


def test_literal_invalid() -> None:
    """Test that invalid Literal usage raises appropriate TaskExceptions."""
    with pytest.raises(TaskException, match="missing a required argument: 'foo'"):
        literal_task(baz="medium")  # type: ignore
    with pytest.raises(
        TaskException,
        match="got invalid type for argument 'baz': expected \\('fast', 'slow'\\), got str or 'medium'",
    ):
        literal_task(foo=4.1, bar=3, baz="medium", qux="test")  # type: ignore
    with pytest.raises(
        TaskException, match="Calling literal_task: Arguments must _always_ be named"
    ):
        literal_task(4.1, 3, "medium", "test")  # type: ignore
    with pytest.raises(TaskException, match="missing a required argument: 'foo'"):
        literal_task()  # type: ignore
    with (
        set_configuration(allow_positional_args=True),
        pytest.raises(TaskException, match="got invalid type for argument 'foo'"),
    ):
        literal_task(1, "fast", "medium")  # type: ignore


# --- Union ---
class UnionConfig(TypedDict):
    """TypedDict for testing dewret @task() argument validation with Union."""

    foo: Union[int, str]
    bar: Optional[str]
    baz: Union[int, Literal["fast", "slow"]]
    # Default values not allowed with mypy. So we ignore.
    qux: Optional[str] = "default"  # type: ignore


@task()
def union_task(**config: Unpack[UnionConfig]) -> dict[str, Any]:
    """Task that accepts Union and Optional fields."""
    return {**config}


def test_union_valid() -> None:
    """Test that valid Union and Optional fields pass correctly."""
    result = union_task(foo=42, bar="hello", baz="fast")  # type: ignore
    wf = construct(result, simplify_ids=True)
    rendered = render(wf)["__root__"]
    assert rendered == yaml.safe_load("""
class: Workflow
cwlVersion: 1.2
inputs:
  union_task-1-bar:
    default: hello
    label: bar
    type: string
  union_task-1-baz:
    default: fast
    label: baz
    type: string
  union_task-1-foo:
    default: 42
    label: foo
    type: int
  union_task-1-qux:
    default: default
    label: qux
    type: string
outputs:
  out:
    label: out
    outputSource: union_task-1/out
    type: record
steps:
  union_task-1:
    in:
      bar:
        source: union_task-1-bar
      baz:
        source: union_task-1-baz
      foo:
        source: union_task-1-foo
      qux:
        source: union_task-1-qux
    out:
    - out
    run: union_task
    """)


def test_union_valid_with_positional_args_true() -> None:
    """Test Union support with positional arguments enabled."""
    with set_configuration(allow_positional_args=True):
        result = union_task(42, "hello", "fast")  # type: ignore
        wf = construct(result, simplify_ids=True)
        rendered = render(wf)["__root__"]
        assert rendered == yaml.safe_load("""
class: Workflow
cwlVersion: 1.2
inputs:
  union_task-1-bar:
    default: hello
    label: bar
    type: string
  union_task-1-baz:
    default: fast
    label: baz
    type: string
  union_task-1-foo:
    default: 42
    label: foo
    type: int
  union_task-1-qux:
    default: default
    label: qux
    type: string
outputs:
  out:
    label: out
    outputSource: union_task-1/out
    type: record
steps:
  union_task-1:
    in:
      bar:
        source: union_task-1-bar
      baz:
        source: union_task-1-baz
      foo:
        source: union_task-1-foo
      qux:
        source: union_task-1-qux
    out:
    - out
    run: union_task
    """)


def test_union_invalid_str() -> None:
    """Test invalid types or missing Union fields raise appropriate TaskExceptions."""
    with pytest.raises(
        TaskException,
        match="got invalid type for argument 'bar': expected \\(<class 'str'>, <class 'NoneType'>\\), got int or 123",
    ):
        union_task(foo=42, bar=123, baz="fast")  # type: ignore
    with pytest.raises(
        TaskException,
        match="missing a value for keyword argument: 'bar' 'bar' is an Optional argument, but no default value provided.",
    ):
        union_task(foo=42, baz="fast")  # type: ignore
