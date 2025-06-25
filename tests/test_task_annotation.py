"""Verify that the task decorator works with TypedDict and NotRequired fields."""

import pytest
import yaml
from dewret.core import set_configuration
from typing import TypedDict, NotRequired, Unpack, Any, Literal
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


# --- Literal ---
class LiteralConfig(TypedDict):
    mode: Literal["fast", "slow"]
    mode1: Literal["fast", "medium", "slow"]


@task()
def literal_task(t1: float, t2: int, **config: Unpack[LiteralConfig]) -> str:
    return config["mode"]


def test_literal_valid() -> None:
    result = literal_task(t1=3.14, t2=1, mode="fast", mode1="medium")
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]
    assert rendered == yaml.safe_load("""
class: Workflow
cwlVersion: 1.2
inputs:
  literal_task-1-mode:
    default: fast
    label: mode
    type: string
  literal_task-1-mode1:
    default: medium
    label: mode1
    type: string
  literal_task-1-t1:
    default: 3.14
    label: t1
    type: float
  literal_task-1-t2:
    default: 1
    label: t2
    type: int
outputs:
  out:
    label: out
    outputSource: literal_task-1/out
    type: string
steps:
  literal_task-1:
    in:
      mode:
        source: literal_task-1-mode
      mode1:
        source: literal_task-1-mode1
      t1:
        source: literal_task-1-t1
      t2:
        source: literal_task-1-t2
    out:
      - out
    run: literal_task

        """)


def test_literal_valid_with_positional_args_true() -> None:
    with set_configuration(allow_positional_args=True):
        result = literal_task(3.14, 1, "fast", "medium")
        workflow = construct(result, simplify_ids=True)
        rendered = render(workflow)["__root__"]
        assert rendered == yaml.safe_load("""
class: Workflow
cwlVersion: 1.2
inputs:
  literal_task-1-mode:
    default: fast
    label: mode
    type: string
  literal_task-1-mode1:
    default: medium
    label: mode1
    type: string
  literal_task-1-t1:
    default: 3.14
    label: t1
    type: float
  literal_task-1-t2:
    default: 1
    label: t2
    type: int
outputs:
  out:
    label: out
    outputSource: literal_task-1/out
    type: string
steps:
  literal_task-1:
    in:
      mode:
        source: literal_task-1-mode
      mode1:
        source: literal_task-1-mode1
      t1:
        source: literal_task-1-t1
      t2:
        source: literal_task-1-t2
    out:
      - out
    run: literal_task

        """)


def test_literal_invalid() -> None:
    with pytest.raises(TaskException, match="missing a required argument: 't1'"):
        literal_task(mode="medium")  # type: ignore
    with pytest.raises(
        TaskException,
        match="got invalid literal value for argument 'mode': expected one of \\('fast', 'slow'\\), got 'medium'",
    ):
        literal_task(t1=4.1, t2=3, mode="medium", mode1="test")  # type: ignore
    with pytest.raises(
        TaskException, match="Calling literal_task: Arguments must _always_ be named"
    ):
        literal_task(4.1, 3, "medium", "test")  # type: ignore
    with pytest.raises(TaskException, match="missing a required argument: 't1'"):
        literal_task()  # type: ignore
    with set_configuration(allow_positional_args=True) and pytest.raises(
        TaskException, match="missing a required argument: 't1'"
    ):
        literal_task(3.14, 1, "fast", "medium")  # type: ignore


def test_literal_invalid2() -> None:
    with pytest.raises(
        TaskException, match="Calling literal_task: Arguments must _always_ be named"
    ):
        # Ignoring type checking here since we're testing invalid input
        literal_task("medium")  # type: ignore


def test_literal_invalid3() -> None:
    with set_configuration(allow_positional_args=True):
        # Ignoring type checking here since we're testing invalid input
        literal_task(4.14, 2, "fast", "fast", "slow", "fast", "fast")  # type: ignore
        assert 3 == 4


def test_literal_invalid4() -> None:
    with set_configuration(eager=True):
        # Ignoring type checking here since we're testing invalid input
        literal_task(mode=1, mode1=2, mode2=3, mode3=4, mode4=5)  # type: ignore
        assert 3 == 4


# # --- Union ---
# class UnionConfig(TypedDict):
#     value: Union[int, str]

# @task()
# def union_task(**config: Unpack[UnionConfig]) -> Union[int, str]:
#     return config["value"]

# def test_union_valid_int() -> None:
#     with set_configuration(eager=True):
#       assert union_task(value=5) == 5

# def test_union_valid_str() -> None:
#     with set_configuration(eager=True):
#       assert union_task(value="hello") == "hello"

# def test_union_invalid_type() -> None:
#     with set_configuration(eager=True) and pytest.raises(TaskException, match="got invalid type for argument 'value'"):
#         union_task(value=3.14)  # type: ignore

# # --- Optional ---
# class OptionalConfig(TypedDict):
#     maybe: Optional[int]

# @task()
# def optional_task(**config: Unpack[OptionalConfig]) -> Optional[int]:
#     return config["maybe"]

# def test_optional_none() -> None:
#     with set_configuration(eager=True):
#         # Test that None is handled correctly
#         assert optional_task(maybe=None) is None

# def test_optional_valid() -> None:
#     with set_configuration(eager=True):
#       assert optional_task(maybe=10) == 10

# # --- List/Dict ---
# class ListDictConfig(TypedDict):
#     items: list[int]
#     metadata: dict[str, str]

# @task()
# def collection_task(**config: Unpack[ListDictConfig]) -> Any:
#     return config

# def test_valid_collections() -> None:
#     result = collection_task(items=[1, 2, 3], metadata={"key": "value"}) == {
#             "items": [1, 2, 3],
#             "metadata": {"key": "value"},
#         }
#     output = construct(result, simplify_ids=True)
#     rendered = render(output)["__root__"]
#     print(yaml.dump(rendered, indent=2))
#     with set_configuration(eager=True):
#         assert collection_task(items=[1, 2, 3], metadata={"key": "value"}) == {
#             "items": [1, 2, 3],
#             "metadata": {"key": "value"},
#         }

# def test_invalid_list_type() -> None:
#     with set_configuration(eager=True) and pytest.raises(TaskException, match="got invalid type for argument 'items'"):
#         collection_task(items="not-a-list", metadata={"x": "y"})  # type: ignore

# # --- Callable ---
# class CallableConfig(TypedDict):
#     cb: Callable[[int], str]

# @task()
# def callable_task(**config: Unpack[CallableConfig]) -> str:
#     return config

# def test_valid_callable() -> None:
#     assert callable_task(cb=lambda x: f"Num: {x}") == "Num: 123"

# def test_invalid_callable() -> None:
#     with pytest.raises(TaskException, match="got invalid type for argument 'cb'"):
#         callable_task(cb=123)  # type: ignore

# # --- Annotated ---
# class AnnotatedConfig(TypedDict):
#     value: Annotated[int, "Some note"]

# @task()
# def annotated_task(**config: Unpack[AnnotatedConfig]) -> int:
#     return config["value"]

# def test_annotated_valid() -> None:
#     assert annotated_task(value=42) == 42
