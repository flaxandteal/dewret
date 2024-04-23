"""Test for expected errors."""

import pytest
from dewret.workflow import Task, Lazy
from dewret.tasks import construct, task, nested_task, TaskException


@task()  # This is expected to be the line number shown below.
def add_task(left: int, right: int) -> int:
    """Adds two values and returns the result."""
    return left + right


ADD_TASK_LINE_NO = 8


@nested_task()
def badly_add_task(left: int, right: int) -> int:
    """Badly attempts to add two numbers."""
    return add_task(left=left)  # type: ignore


@task()
def badly_wrap_task() -> int:
    """Sums two values but should not be calling a task."""
    return add_task(left=3, right=4)


class MyStrangeClass:
    """Dummy class for tests."""

    def __init__(self, task: Task):
        """Dummy constructor for tests."""
        ...


@nested_task()
def unacceptable_object_usage() -> int:
    """Invalid use of custom object within nested task."""
    return MyStrangeClass(add_task(left=3, right=4))  # type: ignore


@nested_task()
def unacceptable_nested_return(int_not_global: bool) -> int | Lazy:
    """Bad nested_task that fails to return a task."""
    add_task(left=3, right=4)
    return 7 if int_not_global else ADD_TASK_LINE_NO


def test_missing_arguments_throw_error() -> None:
    """Check whether omitting a required argument will give an error.

    Since we do not run the original function, it is up to dewret to check
    that the signature is, at least, acceptable to Python.

    WARNING: in keeping with Python principles, this does not error if types
    mismatch, but `mypy` should. You **must** type-check your code to catch these.
    """
    result = add_task(left=3)  # type: ignore
    with pytest.raises(TaskException) as exc:
        construct(result)
    end_section = str(exc.getrepr())[-500:]
    assert str(exc.value) == "missing a required argument: 'right'"
    assert "Task add_task declared in <module> at " in end_section
    assert f"test_errors.py:{ADD_TASK_LINE_NO}" in end_section


def test_missing_arguments_throw_error_in_nested_task() -> None:
    """Check whether omitting a required argument within a nested_task will give an error.

    Since we do not run the original function, it is up to dewret to check
    that the signature is, at least, acceptable to Python.

    WARNING: in keeping with Python principles, this does not error if types
    mismatch, but `mypy` should. You **must** type-check your code to catch these.
    """
    result = badly_add_task(left=3, right=4)
    with pytest.raises(TaskException) as exc:
        construct(result)
    end_section = str(exc.getrepr())[-500:]
    assert str(exc.value) == "missing a required argument: 'right'"
    assert "def badly_add_task" in end_section
    assert "Task add_task declared in <module> at " in end_section
    assert f"test_errors.py:{ADD_TASK_LINE_NO}" in end_section


def test_positional_arguments_throw_error() -> None:
    """Check whether unnamed (positional) arguments throw an error.

    We can use default and non-default arguments, but we expect them
    to _always_ be named.
    """
    result = add_task(3, right=4)
    with pytest.raises(TaskException) as exc:
        construct(result)
    assert (
        str(exc.value)
        .strip()
        .startswith("Calling add_task: Arguments must _always_ be named")
    )


def test_nesting_non_nested_tasks_throws_error() -> None:
    """Ensure nesting is only allow in nested_tasks.

    Nested tasks must be evaluated at construction time, and there
    is no concept of task calls that are not resolved during construction, so
    a task should not be called inside a non-nested task.
    """
    result = badly_wrap_task()
    with pytest.raises(TaskException) as exc:
        construct(result)
    assert (
        str(exc.value)
        .strip()
        .startswith(
            "You referenced a task add_task inside another task badly_wrap_task, but it is not a nested_task"
        )
    )


def test_normal_objects_cannot_be_used_in_nested_tasks() -> None:
    """Most entities cannot appear in a nested_task, ensure we catch them.

    Since the logic in nested tasks has to be embedded explicitly in the workflow,
    complex types are not necessarily representable, and in most cases, we would not
    be able to guarantee that the libraries, versions, etc. match.

    Note: this may be mitigated with sympy support, to some extent.
    """
    result = unacceptable_object_usage()
    with pytest.raises(TaskException) as exc:
        construct(result)
    assert (
        str(exc.value)
        == "Nested tasks must now only refer to global parameters, raw or tasks, not objects: MyStrangeClass"
    )


def test_nested_tasks_must_return_a_task() -> None:
    """Ensure nested tasks are lazy-evaluatable.

    A graph only makes sense if the edges connect, and nested tasks must therefore chain.
    As such, a nested task must represent a real subgraph, and return a node to pull it into
    the main graph.
    """
    result = unacceptable_nested_return(int_not_global=True)
    with pytest.raises(TaskException) as exc:
        construct(result)
    assert (
        str(exc.value)
        == "Task unacceptable_nested_return returned output of type <class 'int'>, which is not a lazy function for this backend."
    )

    result = unacceptable_nested_return(int_not_global=False)
    with pytest.raises(TaskException) as exc:
        construct(result)
    assert (
        str(exc.value)
        == "Task unacceptable_nested_return returned output of type <class 'int'>, which is not a lazy function for this backend."
    )
