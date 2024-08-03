import inspect
from functools import lru_cache
from typing import Protocol, Any, TypeVar, Generic, cast, Literal, TypeAliasType, Annotated, Callable, get_origin, get_args

T = TypeVar("T")
AtConstruct = Annotated[T, "AtConstruct"]

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
    @lru_cache
    def all_annotations(self):
        try:
            self._annotations = self.fn.__globals__["__annotations__"]
        except KeyError:
            self._annotations = {}

        self._annotations.update(self.fn.__annotations__)

        return self._annotations

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
        return self.argument_has(arg, AtConstruct)
