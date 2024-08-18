from dataclasses import dataclass
import base64
from typing import Generic, TypeVar, Protocol, Iterator, Unpack, TypedDict, NotRequired, Generator, Union, Any, get_args, get_origin, Annotated
from contextlib import contextmanager
from contextvars import ContextVar
from sympy import Expr, Symbol

BasicType = str | float | bool | bytes | int | None
RawType = Union[BasicType, list["RawType"], dict[str, "RawType"]]
FirmType = BasicType | list["FirmType"] | dict[str, "FirmType"] | tuple["FirmType", ...]

U = TypeVar("U")

def strip_annotations(parent_type: type) -> tuple[type, tuple]:
    # Strip out any annotations. This should be auto-flattened, so in theory only one iteration could occur.
    metadata = []
    while get_origin(parent_type) is Annotated:
        parent_type, *parent_metadata = get_args(parent_type)
        metadata += list(parent_metadata)
    return parent_type, tuple(metadata)

class WorkflowProtocol(Protocol):
    ...

class UnevaluatableError(Exception):
    ...


class ConstructConfiguration(TypedDict):
    flatten_all_nested: NotRequired[bool]
    allow_positional_args: NotRequired[bool]
    allow_plain_dict_fields: NotRequired[bool]
    field_separator: NotRequired[str]

CONSTRUCT_CONFIGURATION: ContextVar[ConstructConfiguration] = ContextVar("construct-configuration")

@contextmanager
def set_configuration(**kwargs: Unpack[ConstructConfiguration]):
    try:
        previous = ConstructConfiguration(**CONSTRUCT_CONFIGURATION.get())
    except LookupError:
        previous = ConstructConfiguration(
            flatten_all_nested=False,
            allow_positional_args=False,
            allow_plain_dict_fields=False,
            field_separator="/"
        )
        CONSTRUCT_CONFIGURATION.set({})

    try:
        CONSTRUCT_CONFIGURATION.get().update(previous)
        CONSTRUCT_CONFIGURATION.get().update(kwargs)

        yield CONSTRUCT_CONFIGURATION
    finally:
        CONSTRUCT_CONFIGURATION.set(previous)

def get_configuration(key: str):
    return CONSTRUCT_CONFIGURATION.get()[key]

class Reference(Generic[U], Symbol):
    """Superclass for all symbolic references to values."""

    _type: type[U] | None = None
    __workflow__: WorkflowProtocol

    def __init__(self, *args, typ: type[U] | None = None, **kwargs):
        self._type = typ
        super().__init__()

    @property
    def name(self):
        return self.__name__

    def __new__(cls, *args, **kwargs):
        instance = Expr.__new__(cls)
        instance._assumptions0 = {}
        return instance

    @property
    def __root_name__(self) -> str:
        raise NotImplementedError(
            "Reference must have a '__root_name__' property or override '__name__'"
        )

    @property
    def __type__(self):
        if self._type is not None:
            return self._type
        raise NotImplementedError()

    def _raise_unevaluatable_error(self):
        raise UnevaluatableError(f"This reference, {self.__name__}, cannot be evaluated during construction.")

    def __eq__(self, other) -> bool:
        if isinstance(other, list) or other is None:
            return False
        if not isinstance(other, Reference):
            self._raise_unevaluatable_error()
        return super().__eq__(other)

    def __float__(self) -> bool:
        self._raise_unevaluatable_error()
        return False

    def __int__(self) -> bool:
        self._raise_unevaluatable_error()
        return False

    def __bool__(self) -> bool:
        self._raise_unevaluatable_error()
        return False

    @property
    def __name__(self) -> str:
        """Referral name for this reference."""
        workflow = self.__workflow__
        name = self.__root_name__
        return workflow.remap(name)

    def __str__(self) -> str:
        """Global description of the reference."""
        return self.__name__

class IterableMixin(Reference[U]):
    __fixed_len__: int | None = None

    def __init__(self, typ: type[U] | None=None, **kwargs):
        base = strip_annotations(typ)[0]
        super().__init__(typ=typ, **kwargs)
        if get_origin(base) == tuple and (args := get_args(base)):
            # In the special case of an explicitly-typed tuple, we can state a length.
            self.__fixed_len__ = len(args)

    def __len__(self):
        return self.__fixed_len__

    def __iter__(self):
        for count, _ in enumerate(self.__inner_iter__()):
            yield super().__getitem__(count)

    def __inner_iter__(self) -> Generator[Any, None, None]:
        if self.__fixed_len__ is not None:
            for i in range(self.__fixed_len__):
                yield i
        else:
            while True:
                yield None

    def __getitem__(self, attr: str | int) -> Reference[U]:
        return super().__getitem__(attr)

class IteratedGenerator(Generic[U]):
    __wrapped__: Reference[U]

    def __init__(self, to_wrap: Reference[U]):
        self.__wrapped__ = to_wrap

    def __iter__(self):
        count = -1
        for _ in self.__wrapped__.__inner_iter__():
            yield Iterated(to_wrap=self.__wrapped__, iteration=(count := count + 1))


class Iterated(Reference[U]):
    __wrapped__: Reference[U]
    __iteration__: int

    def __init__(self, to_wrap: Reference[U], iteration: int, *args, **kwargs):
        self.__wrapped__ = to_wrap
        self.__iteration__ = iteration
        super().__init__(*args, **kwargs)

    @property
    def _(self):
        return self.__wrapped__._

    @property
    def __root_name__(self) -> str:
        return f"{self.__wrapped__.__root_name__}[{self.__iteration__}]"

    @property
    def __type__(self) -> type:
        return Iterator[self.__wrapped__.__type__]

    def __hash__(self) -> int:
        return hash(self.__root_name__)

    @property
    def __field__(self) -> tuple[str]:
        return tuple(list(self.__wrapped__.__field__) + [str(self.__iteration__)])

    @property
    def __workflow__(self) -> WorkflowProtocol:
        return self.__wrapped__.__workflow__

    @__workflow__.setter
    def __workflow__(self, workflow: WorkflowProtocol) -> None:
        self.__wrapped__.__workflow__ = workflow


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
