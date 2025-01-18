"""Test for expected errors."""

import pytest
from dewret.workflow import Task, Lazy
from dewret.tasks import construct, task, workflow, TaskException
from dewret.annotations import AtRender
from ._lib.extra import increment, pi, reverse_list  # noqa: F401


@task()  # This is expected to be the line number shown below.
def add_task(left: int, right: int) -> int:
    """Adds two values and returns the result."""
    return left + right


ADD_TASK_LINE_NO: int = 10


@workflow()
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


@task()
def pi_exported_from_math() -> float:
    """Get pi from math package by name."""
    from math import pi

    return pi


@task()
def try_recursive() -> float:
    """Get pi from math package by name."""
    return try_recursive()


@task()
def pi_hidden_by_math() -> float:
    """Recursive call with a bug, that we cannot spot because the math library confounds our check."""
    import math  # noqa: F401

    return pi()


@task()
def pi_hidden_by_math_2() -> float:
    """Recursive call with a bug, that we cannot spot because the math library confounds our check."""
    math = 1  # noqa: F841
    return pi()  # noqa: F821


@task()
def pi_with_visible_module_task() -> float:
    """Imported task that _will_ be spotted."""
    from ._lib import extra

    return extra.increment(2)


@task()
def pi_with_invisible_module_task() -> float:
    """Imported task that will _not_ be spotted."""
    from ._lib import extra

    return extra.double(3.14 / 2)


@workflow()
def unacceptable_object_usage() -> int:
    """Invalid use of custom object within nested task."""
    return MyStrangeClass(add_task(left=3, right=4))  # type: ignore


@workflow()
def unacceptable_nested_return(int_not_global: AtRender[bool]) -> int | Lazy:
    """Bad subworkflow that fails to return a task."""
    add_task(left=3, right=4)
    return 7 if int_not_global else ADD_TASK_LINE_NO


def test_missing_arguments_throw_error() -> None:
    """Check whether omitting a required argument will give an error.

    Since we do not run the original function, it is up to dewret to check
    that the signature is, at least, acceptable to Python.

    WARNING: in keeping with Python principles, this does not error if types
    mismatch, but `mypy` should. You **must** type-check your code to catch these.
    """
    with pytest.raises(TaskException) as exc:
        add_task(left=3)  # type: ignore
    end_section = str(exc.getrepr())[-500:]
    assert str(exc.value) == "missing a required argument: 'right'"
    assert "Task add_task declared in <module> at " in end_section
    assert f"test_errors.py:{ADD_TASK_LINE_NO}" in end_section


def test_missing_arguments_throw_error_in_subworkflow() -> None:
    """Check whether omitting a required argument within a subworkflow will give an error.

    Since we do not run the original function, it is up to dewret to check
    that the signature is, at least, acceptable to Python.

    WARNING: in keeping with Python principles, this does not error if types
    mismatch, but `mypy` should. You **must** type-check your code to catch these.
    """
    with pytest.raises(TaskException) as exc:
        badly_add_task(left=3, right=4)
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
    with pytest.raises(TaskException) as exc:
        add_task(3, right=4)
    assert (
        str(exc.value)
        .strip()
        .startswith("Calling add_task: Arguments must _always_ be named")
    )


def test_nesting_non_subworkflows_throws_error() -> None:
    """Ensure nesting is only allow in subworkflows.

    Nested tasks must be evaluated at construction time, and there
    is no concept of task calls that are not resolved during construction, so
    a task should not be called inside a non-nested task.
    """
    with pytest.raises(TaskException) as exc:
        badly_wrap_task()
    assert (
        str(exc.value)
        .strip()
        .startswith(
            "You referenced a task add_task inside another task badly_wrap_task, but it is not a workflow"
        )
    )


def test_nesting_does_not_identify_imports_as_nesting() -> None:
    """Ensure we do not throw errors simply because a task's name appears in an import.

    Non-nested tasks should error if a task appears in the body, but due to https://bugs.python.org/issue36697
    this can be misidentified using getclosurevars.

    TODO: The invisible vs visible difference below is an example of a known bug.
    It seems better to have false negatives than positives, but if an import has not
    already put the module into the sys.modules, then we cannot spot it with this workaround.
    The hidden-by-math examples are also wrong - by inspect module alone, we are not sure if `math` is an import
    and (even if it's not _too_ hard to work that out) whether the variable pi actually comes from it.

    UPDATE (2025/01/18): The visible version strictly succeeded by a mischaracterization
    of getclosurevars, where an imported symbol was conflated with a global that shared its name.
    Unfortunately, addressing that (as 3.12.8 does) makes it harder to spot incorrect task use from modules,
    but we do not wish to execute an inline import to confirm either way, not knowing if the module is
    available or would otherwise be imported during the rendering. A potential improvement would at least
    check whether the module has been loaded and look, and the logic required to link an imported symbol
    to the full module path (in most circumstances) is now in FunctionAnalyser.unbound. The next step
    would be to assess the reliability of matching those modules to existing imports, and the footgun of
    false negatives when the module has never been imported outside a task (or before hitting that definition).

    One direction to go with this would be to see how mypy follows this during inspection and see if we could
    take the same approach.
    """
    good = [
        pi,
        pi_exported_from_math,
        pi_with_invisible_module_task,
        pi_with_visible_module_task,
        pi_hidden_by_math,
        pi_hidden_by_math_2,
    ]
    bad = [try_recursive]
    for tsk in bad:
        with pytest.raises(TaskException) as exc:
            tsk()
        assert str(exc.value).strip().startswith("You referenced a task")
    for tsk in good:
        result = tsk()
        construct(result)


def test_normal_objects_cannot_be_used_in_subworkflows() -> None:
    """Most entities cannot appear in a subworkflow, ensure we catch them.

    Since the logic in nested tasks has to be embedded explicitly in the workflow,
    complex types are not necessarily representable, and in most cases, we would not
    be able to guarantee that the libraries, versions, etc. match.

    Note: this may be mitigated with sympy support, to some extent.
    """
    with pytest.raises(TaskException) as exc:
        unacceptable_object_usage()
    assert (
        str(exc.value)
        == "Attempted to build a workflow from a return-value/result/expression with no references."
    )


def test_subworkflows_must_return_a_task() -> None:
    """Ensure nested tasks are lazy-evaluatable.

    A graph only makes sense if the edges connect, and nested tasks must therefore chain.
    As such, a nested task must represent a real subgraph, and return a node to pull it into
    the main graph.
    """
    with pytest.raises(TaskException) as exc:
        result = unacceptable_nested_return(int_not_global=True)
        construct(result)
    assert (
        str(exc.value)
        == "Attempted to build a workflow from a return-value/result/expression with no references."
    )

    result = unacceptable_nested_return(int_not_global=False)
    construct(result)


bad_num = 3
good_num: int = 4


def test_must_annotate_global() -> None:
    """TODO: Docstrings."""
    worse_num = 3

    @workflow()
    def check_annotation() -> int | float:
        return increment(num=bad_num)

    with pytest.raises(TaskException) as exc:
        check_annotation()

    assert (
        str(exc.value)
        == "Could not find a type annotation for bad_num for check_annotation"
    )

    @workflow()
    def check_annotation_2() -> int | float:
        return increment(num=worse_num)

    with pytest.raises(TaskException) as exc:
        check_annotation_2()

    assert (
        str(exc.value)
        == "Cannot use free variables - please put worse_num at the global scope"
    )

    @workflow()
    def check_annotation_3() -> int | float:
        return increment(num=good_num)

    check_annotation_3()
