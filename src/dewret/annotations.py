import inspect
from functools import lru_cache
from types import FunctionType
from typing import Protocol, Any, TypeVar, Generic, cast, Literal, TypeAliasType, Annotated, Callable, get_origin, get_args, Mapping

T = TypeVar("T")
AtRender = Annotated[T, "AtRender"]

class FunctionAnalyser:
    _fn: Callable[..., Any]
    _annotations: dict[str, Any]

    def __init__(self, fn: Callable[..., Any]):
        self.fn = (
            fn.__init__
            if inspect.isclass(fn) else
            fn.__func__
            if inspect.ismethod(fn) else
            fn
        )

    @property
    def all_annotations(self):
        try:
            self._annotations = self.fn.__globals__["__annotations__"]
        except KeyError:
            self._annotations = {}

        self._annotations.update(self.fn.__annotations__)

        return self._annotations

    @property
    def return_type(self):
        return inspect.signature(inspect.unwrap(self.fn)).return_annotation

    @staticmethod
    def _typ_has(typ: type, annotation: type) -> bool:
        if not hasattr(annotation, "__metadata__"):
           return False
        if (origin := get_origin(typ)):
            if origin is Annotated and hasattr(typ, "__metadata__") and typ.__metadata__ == annotation.__metadata__:
                return True
        if any(FunctionAnalyser._typ_has(arg, annotation) for arg in get_args(typ)):
            return True
        return False

    def argument_has(self, arg: str, annotation: type) -> bool:
        if arg in self.all_annotations:
            typ = self.all_annotations[arg]
            if self._typ_has(typ, annotation):
                return True
        return False

    def is_at_construct_arg(self, arg: str) -> bool:
        return self.argument_has(arg, AtRender)

    @property
    def globals(self) -> Mapping[str, Any]:
        try:
            fn_globals = inspect.getclosurevars(self.fn).globals
        # This covers the case of wrapping, rather than decorating.
        except TypeError:
            fn_globals = {}
        return fn_globals

    def with_new_globals(self, new_globals: dict[str, Any]) -> Callable[..., Any]:
        code = self.fn.__code__
        fn_name = self.fn.__name__
        all_globals = dict(self.globals)
        all_globals.update(new_globals)
        return FunctionType(
            code,
            all_globals,
            name=fn_name,
            closure=self.fn.__closure__,
            argdefs=self.fn.__defaults__,
        )
