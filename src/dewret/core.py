# Copyright 2024- Flax & Teal Limited. All Rights Reserved.
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

"""Base classes that need to be available everywhere.

Mainly tooling around configuration, protocols and superclasses for References
and WorkflowComponents, that are concretized elsewhere.
"""

from dataclasses import dataclass
import importlib
import base64
from attrs import define
from functools import lru_cache
from typing import (
    Generic,
    TypeVar,
    Protocol,
    Iterator,
    Unpack,
    TypedDict,
    NotRequired,
    Generator,
    Any,
    get_args,
    get_origin,
    Annotated,
    Callable,
    cast,
    runtime_checkable,
)
from contextlib import contextmanager
from contextvars import ContextVar
from sympy import Expr, Symbol, Basic
import copy

BasicType = str | float | bool | bytes | int | None
RawType = BasicType | list["RawType"] | dict[str, "RawType"]
FirmType = RawType | list["FirmType"] | dict[str, "FirmType"] | tuple["FirmType", ...]
# Basic is from Sympy, which does not have type annotations, so ExprType cannot pass mypy
ExprType = (FirmType | Basic | list["ExprType"] | dict[str, "ExprType"] | tuple["ExprType", ...])  # type: ignore # fmt: skip

U = TypeVar("U")
T = TypeVar("T")


def strip_annotations(parent_type: type) -> tuple[type, tuple[str]]:
    """Discovers and removes annotations from a parent type.

    Args:
        parent_type: a type, possibly Annotated.

    Returns: a parent type that is not Annotation, along with any stripped metadata, if present.

    """
    # Strip out any annotations. This should be auto-flattened, so in theory only one iteration could occur.
    metadata = []
    while get_origin(parent_type) is Annotated:
        parent_type, *parent_metadata = get_args(parent_type)
        metadata += list(parent_metadata)
    return parent_type, tuple(metadata)


# Generic type for configuration settings for the renderer
RenderConfiguration = dict[str, RawType]


class WorkflowProtocol(Protocol):
    """Expected structure for a workflow.

    We do not expect various workflow implementations, but this allows us to define the
    interface expected by the core classes.
    """

    def remap(self, name: str) -> str:
        """Perform any name-changing for steps, etc. in the workflow.

        This enables, for example, simplifying all the IDs to an integer sequence.

        Returns: remapped name.
        """
        ...

    def set_result(self, result: Basic | list[Basic] | tuple[Basic]) -> None:
        """Set the step that should produce a result for the overall workflow."""
        ...

    def simplify_ids(self, infix: list[str] | None = None) -> None:
        """Drop the non-human-readable IDs if possible, in favour of integer sequences.

        Args:
            infix: any inherited intermediary identifiers, to allow nesting, or None.
        """
        ...


class BaseRenderModule(Protocol):
    """Common routines for all renderer modules."""

    @staticmethod
    def default_config() -> dict[str, RawType]:
        """Retrieve default settings.

        These will not change during execution, but can be overridden by `dewret.core.set_render_configuration`.

        Returns: a static, serializable dict.
        """
        ...


@runtime_checkable
class RawRenderModule(BaseRenderModule, Protocol):
    """Render module that returns raw text."""

    def render_raw(
        self, workflow: WorkflowProtocol, **kwargs: RenderConfiguration
    ) -> dict[str, str]:
        """Turn a workflow into flat strings.

        Returns: one or more subworkflows with a `__root__` key representing the outermost workflow, at least.
        """
        ...


@runtime_checkable
class StructuredRenderModule(BaseRenderModule, Protocol):
    """Render module that returns JSON/YAML-serializable structures."""

    def render(
        self, workflow: WorkflowProtocol, **kwargs: RenderConfiguration
    ) -> dict[str, dict[str, RawType]]:
        """Turn a workflow into a serializable structure.

        Returns: one or more subworkflows with a `__root__` key representing the outermost workflow, at least.
        """
        ...


class RenderCall(Protocol):
    """Callable that will render out workflow(s)."""

    def __call__(
        self, workflow: WorkflowProtocol, **kwargs: RenderConfiguration
    ) -> dict[str, str] | dict[str, RawType]:
        """Take a workflow and turn it into a set of serializable (sub)workflows.

        Args:
            workflow: root workflow.
            kwargs: configuration for the renderer.

        Returns: a mapping of keys to serialized workflows, containing at least `__root__`.
        """
        ...


class UnevaluatableError(Exception):
    """Signposts that a user has tried to treat a reference as the real (runtime) value.

    For example, by comparing to a concrete integer or value, etc.
    """

    ...


@define
class ConstructConfiguration:
    """Basic configuration of the construction process.

    Holds configuration that may be relevant to `construst(...)` calls or, realistically,
    anything prior to rendering. It should hold generic configuration that is renderer-independent.
    """

    flatten_all_nested: bool = False
    allow_positional_args: bool = False
    allow_plain_dict_fields: bool = False
    field_separator: str = "/"
    field_index_types: str = "int"
    simplify_ids: bool = False
    eager: bool = False


class ConstructConfigurationTypedDict(TypedDict):
    """Basic configuration of the construction process.

    Holds configuration that may be relevant to `construst(...)` calls or, realistically,
    anything prior to rendering. It should hold generic configuration that is renderer-independent.

    **THIS MUST BE KEPT IDENTICAL TO ConstructConfiguration.**
    """

    flatten_all_nested: NotRequired[bool]
    allow_positional_args: NotRequired[bool]
    allow_plain_dict_fields: NotRequired[bool]
    field_separator: NotRequired[str]
    field_index_types: NotRequired[str]
    simplify_ids: NotRequired[bool]
    eager: NotRequired[bool]


@define
class GlobalConfiguration:
    """Overall configuration structure.

    Having a single configuration dict allows us to manage only one ContextVar.
    """

    construct: ConstructConfiguration
    render: dict[str, RawType]


CONFIGURATION: ContextVar[GlobalConfiguration] = ContextVar("configuration")


@contextmanager
def set_configuration(
    **kwargs: Unpack[ConstructConfigurationTypedDict],
) -> Iterator[ContextVar[GlobalConfiguration]]:
    """Sets the construct-time configuration.

    This is a context manager, so that a setting can be temporarily overridden and automatically restored.
    """
    with _set_configuration() as CONFIGURATION:
        for key, value in kwargs.items():
            setattr(CONFIGURATION.get().construct, key, value)
        yield CONFIGURATION


@contextmanager
def set_render_configuration(
    kwargs: dict[str, RawType],
) -> Iterator[ContextVar[GlobalConfiguration]]:
    """Sets the render-time configuration.

    This is a context manager, so that a setting can be temporarily overridden and automatically restored.

    Returns: the yielded global configuration ContextVar.
    """
    with _set_configuration() as CONFIGURATION:
        CONFIGURATION.get().render.update(**kwargs)
        yield CONFIGURATION


@contextmanager
def _set_configuration() -> Iterator[ContextVar[GlobalConfiguration]]:
    """Prepares and tidied up the configuration for applying settings.

    This is a context manager, so that a setting can be temporarily overridden and automatically restored.
    """
    try:
        previous = CONFIGURATION.get()
    except LookupError:
        previous = GlobalConfiguration(
            construct=ConstructConfiguration(), render=default_renderer_config()
        )
        CONFIGURATION.set(previous)
    previous = copy.deepcopy(previous)

    try:
        yield CONFIGURATION
    finally:
        CONFIGURATION.set(previous)


@lru_cache
def default_renderer_config() -> RenderConfiguration:
    """Gets the default renderer configuration.

    This may be called frequently, but is cached so note that any changes to the
    wrapped config function will _not_ be reflected during the process.

    It is a light wrapper for `default_config` in the supplier renderer module.

    Returns: the default configuration dict for the chosen renderer.
    """
    try:
        # We have to use a cast as we do not know if the renderer module is valid.
        render_module = cast(
            BaseRenderModule, importlib.import_module("__renderer_mod__")
        )
        default_config: Callable[[], RenderConfiguration] = render_module.default_config
    except ImportError:
        return {}
    return default_config()


@lru_cache
def default_construct_config() -> ConstructConfiguration:
    """Gets the default construct-time configuration.

    This is the primary mechanism for configuring dewret internals, so these defaults
    should be carefully chosen and, if they change, that likely has an impact on backwards compatibility
    from a SemVer perspective.

    Returns: configuration dictionary with default construct values.
    """
    return ConstructConfiguration(
        flatten_all_nested=False,
        allow_positional_args=False,
        allow_plain_dict_fields=False,
        field_separator="/",
        field_index_types="int",
    )


def get_configuration(key: str) -> RawType:
    """Retrieve the configuration or (silently) return the default.

    Helps avoid a proliferation of `set_configuration` calls by not erroring if it has not been called.
    However, the cost is that the user may accidentally put configuration-affected logic outside a
    set_configuration call and be surprised that the behaviour is inexplicibly not as expected.

    Args:
        key: configuration key to retrieve.

    Returns: (preferably) a JSON/YAML-serializable construct.
    """
    try:
        return getattr(CONFIGURATION.get().construct, key)  # type: ignore
    except LookupError:
        # TODO: Not sure what the best way to typehint this is.
        return getattr(ConstructConfiguration(), key)  # type: ignore


def get_render_configuration(key: str) -> RawType:
    """Retrieve configuration for the active renderer.

    Finds the current user-set configuration, defaulting back to the chosen renderer module's declared
    defaults.

    Args:
        key: configuration key to retrieve.

    Returns: (preferably) a JSON/YAML-serializable construct.
    """
    try:
        return CONFIGURATION.get().render.get(key)
    except LookupError:
        return default_renderer_config().get(key)


class WorkflowComponent:
    """Base class for anything directly tied to an individual `Workflow`.

    Attributes:
        __workflow__: the `Workflow` that this is tied to.
    """

    __workflow_real__: WorkflowProtocol

    def __init__(self, *args: Any, workflow: WorkflowProtocol, **kwargs: Any):
        """Tie to a `Workflow`.

        All subclasses must call this.

        Args:
            workflow: the `Workflow` to tie to.
            *args: remainder of arguments for other initializers.
            **kwargs: remainder of arguments for other initializers.
        """
        self.__workflow__ = workflow
        super().__init__(*args, **kwargs)

    @property
    def __workflow__(self) -> WorkflowProtocol:
        """Workflow to which this reference applies."""
        return self.__workflow_real__

    @__workflow__.setter
    def __workflow__(self, workflow: WorkflowProtocol) -> None:
        """Workflow to which this reference applies."""
        self.__workflow_real__ = workflow


class Reference(Generic[U], Symbol, WorkflowComponent):
    """Superclass for all symbolic references to values."""

    _type: type[U] | None = None
    __iterated__: bool = False

    def __init__(self, *args: Any, typ: type[U] | None = None, **kwargs: Any):
        """Extract any specified type.

        Args:
            typ: type to override any inference, or None.
            *args: any other arguments for other initializers (e.g. mixins).
            **kwargs: any other arguments for other initializers (e.g. mixins).
        """
        self._type = typ
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        """Printable name of the reference."""
        return self.__name__

    def __new__(cls, *args: Any, **kwargs: Any) -> "Reference[U]":
        """As all references are sympy Expressions, the real returned object must be made with Expr."""
        instance = Expr.__new__(cls)
        instance._assumptions0 = {}
        return cast(Reference[U], instance)

    @property
    def __root_name__(self) -> str:
        """Root name on which to suffix/prefix any derived names (with fields, etc.).

        For example, the base name of `add_thing-12345[3]` should be `add_thing`.

        Returns: basic name as a string.
        """
        raise NotImplementedError(
            "Reference must have a '__root_name__' property or override '__name__'"
        )

    @property
    def __type__(self) -> type:
        """Type of the reference target, if known."""
        if self._type is not None:
            return self._type
        raise NotImplementedError()

    def _raise_unevaluatable_error(self) -> None:
        """Convenience method to consistently formulate an UnevaluatableError for this reference."""
        raise UnevaluatableError(
            f"This reference, {self.__name__}, cannot be evaluated during construction."
        )

    def __eq__(self, other: object) -> Any:
        """Test equality at construct-time, if sensible.

        Raises:
            UnevaluatableError: if it seems the user is confusing this with a runtime check.
        """
        if isinstance(other, list) or other is None:
            return False
        if not isinstance(other, Reference) and not isinstance(other, Basic):
            self._raise_unevaluatable_error()
        return super().__eq__(other)

    def __float__(self) -> bool:
        """Catch accidental float casts.

        Raises:
            UnevaluatableError: unconditionally, as it seems the user is confusing this with a runtime check.
        """
        self._raise_unevaluatable_error()
        return False

    def __int__(self) -> bool:
        """Catch accidental int casts.

        Raises:
            UnevaluatableError: unconditionally, as it seems the user is confusing this with a runtime check.
        """
        self._raise_unevaluatable_error()
        return False

    def __bool__(self) -> bool:
        """Catch accidental bool casts.

        Note that this means ambiguous checks such as `if ref: ...` will error, and should be `if ref is None: ...`
        or similar.

        Raises:
            UnevaluatableError: unconditionally, as it seems the user is confusing this with a runtime check.
        """
        self._raise_unevaluatable_error()
        return False

    @property
    def __name__(self) -> str:
        """Referral name for this reference.

        Returns: an internal name to refer to the reference target.
        """
        workflow = self.__workflow__
        name = self.__root_name__
        return workflow.remap(name) if workflow is not None else name

    def __str__(self) -> str:
        """Global description of the reference.

        Returns the _internal_ name.
        """
        return self.__name__


class IterableMixin(Reference[U]):
    """Functionality for iterating over references to give new references."""

    __fixed_len__: int | None = None

    def __init__(self, typ: type[U] | None = None, **kwargs: Any):
        """Extract length, if available from type.

        Captures types of the form (e.g.) `tuple[int, float]` and records the length
        as being (e.g.) 2.
        """
        super().__init__(typ=typ, **kwargs)
        base = strip_annotations(self.__type__)[0]
        if get_origin(base) == tuple and (args := get_args(base)):
            # In the special case of an explicitly-typed tuple, we can state a length.
            self.__fixed_len__ = len(args)

    def __len__(self) -> int:
        """Length of this iterable, if available.

        The two cases that this is likely to be available are if the reference target
        has been type-hinted as a `tuple` with a specific, fixed number of type arguments,
        or if the target has been annotated with `Fixed(...)` indicating that the length
        of the default value can be hard-coded into the output, and therefore that it can
        be used for graph-building logic. The most useful application of this is likely to
        be in for-loops and generators, as we can create variable references for each iteration
        but can nonetheless execute the loop as we know how many iterations occur.

        Returns: length of the iterable, if available.
        """
        if self.__fixed_len__ is None:
            raise TypeError(
                "This iterable reference does not have a known fixed length, "
                "consider using `Fixed[...]` with a default, or typehint using `tuple[int, float]` (or similar) "
                "to tell dewret how long it should be."
            )
        return self.__fixed_len__

    def __iter__(self) -> Generator[Reference[U], None, None]:
        """Execute the iteration.

        Note that this does _not_ return the yielded values of `__inner_iter__`, so that
        it can be used with impunity to do actual iteration, and we will _always_ return
        references here.

        Returns: a generator that will give a new reference for every iteration.
        """
        for count, _ in enumerate(self.__inner_iter__()):
            yield super().__getitem__(count)

    def __inner_iter__(self) -> Generator[Any, None, None]:
        """Overrideable iterator for looping over the wrapped object.

        Returns: a generator that will yield ints if this reference is known to be fixed length, or will go
        forever yielding None otherwise.
        """
        if self.__fixed_len__ is not None:
            for i in range(self.__fixed_len__):
                yield i
        else:
            while True:
                yield None

    def __getitem__(self, attr: str | int) -> "Reference[U] | Any":
        """Get a reference to an individual item/field.

        Args:
            attr: index or fieldname.

        Returns: a reference to the same target as this reference but a level deeper.
        """
        return super().__getitem__(attr)


class IteratedGenerator(Generic[U]):
    """Sentinel value for capturing that an iteration has occured without performing it.

    Allows us to lazily evaluate a loop, for instance, in the renderer. This may be relevant
    if the renderer wishes to postpone iteration to runtime, and simply record it is required,
    rather than evaluating the iterator.
    """

    __wrapped__: Reference[U]

    def __init__(self, to_wrap: Reference[U]):
        """Capture wrapped reference.

        Args:
            to_wrap: reference to wrap.
        """
        self.__wrapped__ = to_wrap

    def __iter__(self) -> Generator[Reference[U], None, None]:
        """Loop through the wrapped reference.

        This will tag the references that are returned, so that the renderer can see this has
        happened.

        Returns: a generator looping over the wrapped reference with a counter as the "field".
        """
        count = -1
        for _ in self.__wrapped__.__inner_iter__():
            ref = self.__wrapped__[(count := count + 1)]
            ref.__iterated__ = True
            yield ref


@dataclass
class Raw:
    """Value object for any raw types.

    This is able to hash raw types consistently and provides
    a single type for validating type-consistency.

    Attributes:
        value: the real value, e.g. a `str`, `int`, ...
    """

    value: RawType

    def __hash__(self) -> int:
        """Provide a hash that is unique to the `value` member."""
        return hash(repr(self))

    def __repr__(self) -> str:
        """Convert to a consistent, string representation."""
        value: str
        if isinstance(self.value, bytes):
            value = base64.b64encode(self.value).decode("ascii")
        else:
            value = str(self.value)
        return f"{type(self.value).__name__}|{value}"
