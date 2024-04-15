"""Test for expected errors."""

import pytest
from dewret.tasks import construct, task, nested_task

@task()
def add_task(left: int, right: int) -> int:
    """Adds two values and returns the result."""
    return left + right

@nested_task()
def badly_add_task(left: int, right: int) -> int:
    """Badly attempts to add two numbers."""
    return add_task(left=left) # type: ignore

def test_missing_arguments_throw_error() -> None:
    """Check whether omitting a required argument will give an error.

    Since we do not run the original function, it is up to dewret to check
    that the signature is, at least, acceptable to Python.

    WARNING: in keeping with Python principles, this does not error if types
    mismatch, but `mypy` should. You **must** type-check your code to catch these.
    """
    result = add_task(left=3) # type: ignore
    with pytest.raises(TypeError) as exc:
        construct(result)
    assert str(exc.value) == "missing a required argument: 'right'"

def test_missing_arguments_throw_error_in_nested_task() -> None:
    """Check whether omitting a required argument will give an error.

    Since we do not run the original function, it is up to dewret to check
    that the signature is, at least, acceptable to Python.

    WARNING: in keeping with Python principles, this does not error if types
    mismatch, but `mypy` should. You **must** type-check your code to catch these.
    """
    result = badly_add_task(left=3, right=4)
    with pytest.raises(TypeError) as exc:
        construct(result)
    assert str(exc.value) == "missing a required argument: 'right'"

def test_positional_arguments_throw_error() -> None:
    """Check whether we can produce simple CWL.

    We can use default and non-default arguments, but we expect them
    to _always_ be named.
    """
    result = add_task(3, right=4)
    with pytest.raises(TypeError) as exc:
        construct(result)
    assert str(exc.value) == "Calling add_task: Arguments must _always_ be named, e.g. my_task(num=1) not my_task(1)"
