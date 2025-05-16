# Copyright 2014 Flax & Teal Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Abstraction layer for task operations.

Access dask, or other, backends consistently using this module. It provides
decorators and execution calls that manage tasks. Note that the `task`
decorator should be called with no arguments, and will return the appropriate
decorator for the current backend.

Typical usage example:

```python
>>> @task()
... def increment(num: int) -> int:
...     return num + 1

```
"""

import inspect
import importlib
import sys
from enum import Enum
from functools import cached_property
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, cast, Generator, Unpack, Literal
from types import TracebackType
from attrs import has as attrs_has
from dataclasses import is_dataclass
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar, copy_context
from contextlib import contextmanager

from .utils import is_firm, make_traceback, is_expr
from .workflow import (
    execute_step,
    expr_to_references,
    unify_workflows,
    UNSET,
    Workflow,
    Lazy,
    LazyEvaluation,
    Target,
    LazyFactory,
    Parameter,
    ParameterReference,
    param,
    Task,
    is_task,
)
from .backends._base import BackendModule
from .annotations import FunctionAnalyser
from .core import (
    get_configuration,
    set_configuration,
    IteratedGenerator,
    ConstructConfigurationTypedDict,
    Reference,
)

Param = ParamSpec("Param")
RetType = TypeVar("RetType")
T = TypeVar("T")


class Backend(Enum):
    """Stringy enum representing available backends."""

    DASK = "dask"


DEFAULT_BACKEND = Backend.DASK


class TaskManager:
    """Overarching backend-agnostic task manager.

    Gatekeeps the specific backend implementation. This can be
    instantiated without choosing a backend, but the first call to
    any of its methods will concretize that choice - either as
    the default, or the backend set via `TaskManager.set_backend`.
    It cannot be changed after this point.
    """

    _backend: Backend | None = None

    def set_backend(self, backend: Backend) -> Backend:
        """Choose a backend.

        Sets the backend, provided it has not already been loaded.

        Args:
            backend: chosen backend, to override the default.

        Returns:
            Backend that was set.

        Raises:
            RuntimeError: when a backend has already been loaded.
        """
        if self._backend is not None:
            raise RuntimeError(
                f"Backend is already loaded ({self._backend}). Did an imported module use lazy/run already?"
            )
        self._backend = backend
        return self._backend

    @cached_property
    def backend(self) -> BackendModule:
        """Import backend module.

        Cached property to load the backend module, if it has not been already.

        Returns:
            Backend module for the specific choice of backend.
        """
        backend = self._backend
        if backend is None:
            backend = self.set_backend(DEFAULT_BACKEND)

        backend_mod = importlib.import_module(
            f".backends.backend_{backend.value}", "dewret"
        )
        return backend_mod

    def make_lazy(self) -> LazyFactory:
        """Get the lazy decorator for this backend.

        Returns:
            Real decorator for this backend.
        """
        return self.backend.lazy

    def evaluate(self, task: Lazy | list[Lazy] | tuple[Lazy, ...], __workflow__: Workflow, thread_pool: ThreadPoolExecutor | None=None, **kwargs: Any) -> Any:
        """Evaluate a single task for a known workflow.

        Args:
            task: the task to evaluate.
            __workflow__: workflow within which this exists.
            thread_pool: existing pool of threads to run this in, or None.
            **kwargs: any arguments to pass to the task.
        """
        result = self.backend.run(__workflow__, task, thread_pool=thread_pool, **kwargs)
        new_result, collected_workflow = unify_workflows(result, __workflow__)

        if collected_workflow is None:
            raise RuntimeError("A new workflow could not be found")

        # Then we set the result to be the whole thing
        collected_workflow.set_result(new_result)
        return collected_workflow.result

    def unwrap(self, task: Lazy) -> Target:
        """Unwraps a lazy-evaluated function to get the function.

        Ideally, we could use the `__wrapped__` property but not all
        workflow engines support this, and most importantly, dask has
        only done so as of 2024.03.

        Args:
            task: task to be unwrapped.

        Returns:
            Original target.

        Raises:
            RuntimeError: if the task is not a wrapped function.
        """
        return self.backend.unwrap(task)

    def ensure_lazy(self, task: Any) -> Lazy | None:
        """Evaluate a single task for a known workflow.

        As we mask our lazy-evaluable functions to appear as their original
        types to the type system (see `dewret.tasks.task`), we must cast them
        back, to allow the type-checker to comb the remainder of the code.

        Args:
            task: the suspected task to check.

        Returns:
            Original task, cast to a Lazy, or None.
        """
        if isinstance(task, LazyEvaluation):
            return self.ensure_lazy(task._fn)
        return task if self.backend.is_lazy(task) else None

    def __call__(
        self,
        task: Any,
        __workflow__: Workflow | None = None,
        **kwargs: Unpack[ConstructConfigurationTypedDict],
    ) -> Workflow:
        """Execute the lazy evalution.

        Arguments:
            task: the task to evaluate.
            simplify_ids: when we finish running, make nicer step names?
            **kwargs: any arguments to pass to the task.

        Returns:
            A reusable reference to this individual step.
        """
        workflow = __workflow__ or Workflow()

        with set_configuration(**kwargs):
            context = copy_context().items()

            def _initializer() -> None:
                for var, value in context:
                    var.set(value)

            thread_pool = ThreadPoolExecutor(initializer=_initializer)

            result = self.evaluate(task, workflow, thread_pool=thread_pool, **kwargs)
            simplify_ids = bool(get_configuration("simplify_ids"))
        return Workflow.from_result(result, simplify_ids=simplify_ids)


_manager = TaskManager()
lazy = _manager.make_lazy
ensure_lazy = _manager.ensure_lazy
unwrap = _manager.unwrap

def evaluate(task: Any, *args: Any, execute: bool = False, **kwargs: Any) -> Any:
    """Get a result of a task, either as a value or lazily.

    Args:
        task: task to evaluate
        *args: other arguments to the evaluator
        execute: whether or not to evaluate to obtain the final result
        **kwargs: other arguments to the evaluator

    Returns:
        Structure of lazy evaluations if lazy, else actual result.
    """
    if execute:
        return execute_step(task, *args, **kwargs)
    else:
        return _manager.evaluate(task, *args, **kwargs)

"""An alias pointing to an instance of the TaskManager class.
Used for constructing a set of tasks into a dewret workflow instance.
"""
construct = _manager


class TaskException(Exception):
    """Exception tied to a specific task.

    Primarily aimed at parsing issues, but this will ensure that
    a message is shown with useful debug information for the
    workflow writer.
    """

    def __init__(
        self,
        task: Task | Target,
        dec_tb: TracebackType | None,
        tb: TracebackType | None,
        message: str,
        *args: Any,
        **kwargs: Any,
    ):
        """Create a TaskException for this exception.

        Args:
            task: the Task causing the exception.
            dec_tb: a traceback of the task declaration.
            tb: a traceback of the original task call.
            message: a message to show to the user.
            *args: any other arguments accepted by Exception.
            **kwargs: any other arguments accepted by Exception.
        """
        if dec_tb:
            frame = traceback.extract_tb(dec_tb)[-1]
            self.add_note(
                f"Task {task.__name__} declared in {frame.name} at {frame.filename}:{frame.lineno}\n"
                f"{frame.line}"
            )
        super().__init__(message)
        self.__traceback__ = tb


_IN_NESTED_TASK: ContextVar[bool] = ContextVar("in_nested_task")
_IN_NESTED_TASK.set(False)


@contextmanager
def in_nested_task() -> Generator[None, None, None]:
    """Informs the builder that we are within a nested task.

    This is only really relevant in the subworkflow context.

    TODO: check impact of ContextVar being thread-sensitive on build.
    """
    try:
        tok = _IN_NESTED_TASK.set(True)
        yield
    finally:
        _IN_NESTED_TASK.reset(tok)


def is_in_nested_task() -> bool:
    """Check if we are within a nested task.

    Used, for example, to see if discovered parameters should be
    treated as "local" (i.e. should take a default to the step) or
    global (i.e. should be turned into a workflow parameter) if we
    are inside or outside a subworkflow, respectively.
    """
    try:
        return _IN_NESTED_TASK.get()
    except LookupError:
        return False


def factory(fn: Callable[..., RetType]) -> Callable[..., RetType]:
    """Create a factory, that can be treated as complex input to a workflow.

    Args:
        fn: a callable to create the entity.
    """
    ret = task(is_factory=True)(fn)
    return ret


# Workaround for PyCharm
factory: Callable[[Callable[..., RetType]], Callable[..., RetType]] = factory


def workflow() -> Callable[[Callable[Param, RetType]], Callable[Param, RetType]]:
    """Shortcut for marking a task as nested.

    A nested task is one which calls other tasks and does not
    do anything else important. It will _not_ actually get called
    at runtime, but should map entirely into the graph. As such,
    arithmetic operations on results, etc. will cause errors at
    render-time. Combining tasks is acceptable, and intended. The
    effect of the nested task will be considered equivalent to whatever
    reaching whatever step reference is returned at the end.

    ```python
    >>> @task()
    ... def increment(num: int) -> int:
    ...     return num + 1

    >>> @workflow()
    ... def double_increment(num: int) -> int:
    ...     return increment(increment(num=num))

    ```

    Returns:
        Task that runs at render, not execution, time.
    """
    return task(nested=True, flatten_nested=False)


# Workaround for PyCharm
workflow: Callable[[], Callable[[T], T]] = workflow


def task(
    nested: bool = False,
    flatten_nested: bool = True,
    is_factory: bool = False,
) -> Callable[[Callable[Param, RetType]], Callable[Param, RetType]]:
    """Decorator factory abstracting backend's own task decorator.

    For example:

    ```python
    >>> @task()
    ... def increment(num: int) -> int:
    ...     return num + 1

    ```

    If the backend is `dask` (the default), it is will evaluate this
    as a `dask.delayed`. Note that, with any backend, dewret will
    hijack the decorator to record the attempted _evalution_ rather than
    actually _evaluating_ the lazy function. Nonetheless, this hijacking
    will still be executed with the backend's lazy executor, so
    `dask.delayed` will still be called, for example, in the dask case.

    Args:
        nested: whether this should be executed to find other tasks.
        flatten_nested: (only relevant to nested tasks) should this nested task
            be considered a distinct subworkflow, or is it just organizational
            for the outer workflow.
        is_factory: whether this task should be marked as a 'factory', rather than
            a normal step.

    Returns:
        Decorator for the current backend to mark lazy-executable tasks.

    Raises:
        TypeError: if arguments are missing or incorrect, in line with usual
            Python behaviour.
    """

    def _task(fn: Callable[Param, RetType]) -> Callable[Param, RetType]:
        declaration_tb = make_traceback()

        def _fn(
            *args: Any,
            __workflow__: Workflow | None = None,
            __traceback__: TracebackType | None = None,
            **kwargs: Any,
        ) -> RetType:
            if get_configuration("eager"):
                return fn(*args, **kwargs)

            configuration = None
            allow_positional_args = bool(get_configuration("allow_positional_args"))

            try:
                # Ensure that all arguments are passed as keyword args and prevent positional args.
                # passed at all.
                if args and not allow_positional_args:
                    raise TypeError(
                        f"""
                        Calling {fn.__name__}: Arguments must _always_ be named,
                        e.g. my_task(num=1) not my_task(1)\n"

                        @task()
                        def add_numbers(left: int, right: int):
                            return left + right

                        construct(add_numbers(left=3, right=5))
                        """
                    )

                # Ensure that the passed arguments are, at least, a Python-match for the signature.
                sig = inspect.signature(fn)
                positional_args = {key: False for key in kwargs}
                for arg, (key, _) in zip(args, sig.parameters.items(), strict=False):
                    if isinstance(arg, IteratedGenerator):
                        for inner_arg, (key, _) in zip(
                            arg, sig.parameters.items(), strict=False
                        ):
                            if key in positional_args:
                                continue
                            kwargs[key] = inner_arg
                            positional_args[key] = True
                    else:
                        kwargs[key] = arg
                        positional_args[key] = True
                sig.bind(**kwargs)

                def _to_param_ref(value: Any) -> ParameterReference[Any] | None:
                    if isinstance(value, Parameter):
                        return value.make_reference(workflow=__workflow__)
                    return None

                refs = []
                for key, val in kwargs.items():
                    val, kw_refs = expr_to_references(val, remap=_to_param_ref)
                    refs += kw_refs
                    kwargs[key] = val
                # Not realistically going to be other than Workflow.
                workflows: list[Workflow] = [
                    cast(Workflow, reference.__workflow__)
                    for reference in refs
                    if hasattr(reference, "__workflow__")
                    and reference.__workflow__ is not None
                ]
                if __workflow__ is not None:
                    workflows.insert(0, __workflow__)
                if workflows:
                    workflow = Workflow.assimilate(*workflows)
                else:
                    workflow = Workflow()

                analyser = FunctionAnalyser(fn)

                if not is_in_nested_task():
                    for var, value in kwargs.items():
                        if analyser.is_at_construct_arg(var):
                            kwargs[var] = value
                        elif is_firm(value):
                            # We leave this reference dangling for a consumer to pick up ("tethered"), unless
                            # we are in a nested task, that does not have any existence of its own.
                            tethered: Literal[False] | None = (
                                False
                                if nested
                                and (
                                    flatten_nested
                                    or get_configuration("flatten_all_nested")
                                )
                                else None
                            )
                            kwargs[var] = cast(
                                Parameter[Any],
                                param(
                                    var,
                                    value,
                                    tethered=tethered,
                                    autoname=tethered is not False,
                                    typ=analyser.get_argument_annotation(var) or UNSET,
                                ),
                            ).make_reference(workflow=workflow)
                original_kwargs = dict(kwargs)
                fn_globals = analyser.globals

                for var, value in fn_globals.items():
                    # This error is redundant as it triggers a SyntaxError in Python.
                    # Note: the following test duplicates a syntax error.
                    #   if var in kwargs:
                    #       raise TypeError(
                    #           "Captured parameter {var} (global variable in task) shadows an argument"
                    #        )
                    if isinstance(value, Parameter):
                        kwargs[var] = value.make_reference(workflow=workflow)
                    elif is_task(value) or ensure_lazy(value) is not None:
                        if not nested and _workaround_check_value_is_task(
                            fn, var, value
                        ):
                            raise TypeError(
                                f"""
                                You referenced a task {var} inside another task {fn.__name__}, but it is not a workflow
                                - this will not be found!

                                @task()
                                def {var}(...) -> ...:
                                    ...

                                @workflow() <<<--- likely what you want
                                def {fn.__name__}(...) -> ...:
                                    ...
                                    {var}(...)
                                    ...
                                """
                            )
                    # If nested, we will execute the insides, and it is reasonable and important
                    # to have a full set of annotations for any encountered variables.
                    elif (
                        nested
                        and not analyser.get_argument_annotation(var, exhaustive=True)
                        and not inspect.isclass(value)
                        or inspect.isfunction(value)
                    ):
                        raise RuntimeError(
                            f"Could not find a type annotation for {var} for {fn.__name__}"
                        )
                    elif (
                        analyser.is_at_construct_arg(var, exhaustive=True)
                        or value is evaluate
                        or value is construct  # Allow manual building.
                    ):
                        kwargs[var] = value
                    elif inspect.isclass(value) or inspect.isfunction(value):
                        # We assume these are loaded at runtime.
                        ...
                    elif is_firm(value) or (
                        (attrs_has(value) or is_dataclass(value))
                        and not inspect.isclass(value)
                    ):
                        kwargs[var] = cast(
                            Parameter[Any],
                            param(
                                var,
                                default=value,
                                tethered=False,
                                typ=analyser.get_argument_annotation(
                                    var, exhaustive=True
                                )
                                or UNSET,
                            ),
                        ).make_reference(workflow=workflow)
                    elif (
                        is_expr(value)
                        and (expr_refs := expr_to_references(value))
                        and len(expr_refs[1]) != 0
                    ):
                        kwargs[var] = value
                    elif nested:
                        raise NotImplementedError(
                            f"Nested tasks must now only refer to global parameters, raw or tasks, not objects: {var}"
                        )
                if nested:
                    if flatten_nested or get_configuration("flatten_all_nested"):
                        with in_nested_task():
                            output = analyser.with_new_globals(kwargs)(
                                **original_kwargs
                            )
                        lazy_fn = ensure_lazy(output)
                        if lazy_fn is not None:
                            with in_nested_task():
                                output = evaluate(lazy_fn, __workflow__=workflow)
                            # raise TypeError(
                            #    f"Task {fn.__name__} returned output of type {type(output)}, which is not a lazy function for this backend."
                            # )
                        step_reference = output
                    else:
                        nested_workflow = Workflow(name=fn.__name__)
                        nested_globals: dict[str, Any] = {
                            var: cast(
                                Parameter[Any],
                                param(
                                    var,
                                    default=value.__default__
                                    if hasattr(value, "__default__")
                                    else UNSET,
                                    typ=(value.__type__),
                                    tethered=nested_workflow,
                                ),
                            ).make_reference(workflow=nested_workflow)
                            if isinstance(value, Reference)
                            else value
                            for var, value in kwargs.items()
                        }
                        nested_kwargs = {
                            key: value
                            for key, value in nested_globals.items()
                            if key in original_kwargs
                        }
                        with in_nested_task():
                            output = analyser.with_new_globals(nested_globals)(
                                **nested_kwargs
                            )
                            nested_workflow = _manager(
                                output, __workflow__=nested_workflow
                            )
                        step_reference = workflow.add_nested_step(
                            fn.__name__,
                            nested_workflow,
                            analyser.return_type,
                            original_kwargs,
                            positional_args,
                        )
                    if is_expr(step_reference):
                        return cast(RetType, step_reference)
                    raise TypeError(
                        f"Nested tasks must return a step reference, not {type(step_reference)} to ensure graph makes sense."
                    )
                step = cast(
                    RetType,
                    workflow.add_step(
                        fn,
                        kwargs,
                        raw_as_parameter=not is_in_nested_task(),
                        is_factory=is_factory,
                        positional_args=positional_args,
                    ),
                )
                return step
            except TaskException as exc:
                raise exc
            except Exception as exc:
                raise TaskException(
                    fn,
                    declaration_tb,
                    __traceback__,
                    exc.args[0] if exc.args else f"Could not call task {fn.__name__}",
                ) from exc
            finally:
                if configuration:
                    configuration.__exit__(None, None, None)

        _fn.__step_expression__ = True  # type: ignore
        _fn.__original__ = fn  # type: ignore
        return LazyEvaluation(_fn)

    return _task


# Workaround for PyCharm
task: Callable[[], Callable[[T], T]] = task


def set_backend(backend: Backend) -> None:
    """Choose a backend.

    Will raise an error if a backend is already chosen.

    Args:
        backend: chosen backend to use from here-on in.
    """
    _manager.set_backend(backend)


def _workaround_check_value_is_task(
    fn: Callable[..., Any], var: str, value: Any
) -> bool:
    """Workaround for known issue in Python stdlib.

    getclosurevars does not distinguish between globals and inline-imported
    variables. For task-based workflows, this is a much more import use-case
    than in regular Python code, and the bug has been open since 2019, so a
    workaround seems the best course of action.

    This attempts to limit the false positives, by ensuring the "global"
    is not a task in an already-imported module (at least not
    if imported the same name). See `task_errors.py` for more limitations.

    See: https://bugs.python.org/issue36697
    """
    return all(
        (var not in vars(sys.modules[mod]) or is_task(vars(sys.modules[mod])[var]))
        for mod in list(fn.__code__.co_names) + list(fn.__code__.co_varnames)
        if mod in sys.modules
    )
