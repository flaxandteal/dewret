from typing import Generic, TypeVar, Protocol, Iterator, Unpack, TypedDict, NotRequired, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from sympy import Expr, Symbol

U = TypeVar("U")

class WorkflowProtocol(Protocol):
    ...

class UnevaluatableError(Exception):
    ...


class ConstructConfiguration(TypedDict):
    flatten_all_nested: NotRequired[bool]
    allow_positional_args: NotRequired[bool]
    allow_plain_dict_fields: NotRequired[bool]

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
        self.name = self.__root_name__


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

    def __iter__(self) -> Iterator["Reference"]:
        yield IteratedGenerator(self)

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

class IteratedGenerator(Generic[U]):
    __wrapped__: Reference[U]

    def __init__(self, to_wrap: Reference[U]):
        self.__wrapped__ = to_wrap

    def __iter__(self):
        count = -1
        while True:
            yield Iterated(to_wrap=self.__wrapped__, iteration=(count := count + 1))


class Iterated(Reference[U]):
    __wrapped__: Reference[U]
    __iteration__: int

    def __init__(self, to_wrap: Reference[U], iteration: int, *args, **kwargs):
        self.__wrapped__ = to_wrap
        self.__iteration__ = iteration
        super().__init__(*args, **kwargs)

    @property
    def __root_name__(self) -> str:
        return f"{self.__wrapped__.__root_name__}[{self.__iteration__}]"

    @property
    def __type__(self) -> type:
        return Iterator[self.__wrapped__.__type__]

    def __hash__(self) -> int:
        return hash(self.__root_name__)

    def __field__(self) -> str:
        return str(self.__iteration__)

    @property
    def __workflow__(self) -> WorkflowProtocol:
        return self.__wrapped__.__workflow__

    @__workflow__.setter
    def __workflow__(self, workflow: WorkflowProtocol) -> None:
        self.__wrapped__.__workflow__ = workflow
