import inspect
import ast
import sys
import importlib
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

    def get_all_module_names(self):
        return sys.modules[self.fn.__module__].__annotations__

    def get_all_imported_names(self):
        return self._get_all_imported_names(sys.modules[self.fn.__module__])

    @staticmethod
    @lru_cache
    def _get_all_imported_names(mod):
        ast_tree = ast.parse(inspect.getsource(mod))
        imported_names = {}
        for node in ast.walk(ast_tree):
            if isinstance(node, ast.ImportFrom):
                for name in node.names:
                    imported_names[name.asname or name.name] = (
                        importlib.import_module("".join(["."] * node.level) + node.module, package=mod.__package__),
                        name.name
                    )
        return imported_names

    def get_argument_annotation(self, arg: str, exhaustive: bool=False) -> type | None:
        all_annotations: dict[str, type] = {}
        typ: type | None = None
        if (typ := self.fn.__annotations__.get(arg)):
            ...
        elif exhaustive:
            if "__annotations__" in self.fn.__globals__:
                if (typ := self.fn.__globals__["__annotations__"].get(arg)):
                    ...
                elif (orig_pair := self.get_all_imported_names().get(arg)):
                    orig_module, orig_name = orig_pair
                    typ = orig_module.__annotations__.get(orig_name)
        return typ

    def argument_has(self, arg: str, annotation: type, exhaustive: bool=False) -> bool:
        typ = self.get_argument_annotation(arg, exhaustive)
        return bool(typ and self._typ_has(typ, annotation))

    def is_at_construct_arg(self, arg: str, exhaustive: bool=False) -> bool:
        return self.argument_has(arg, AtRender, exhaustive)

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
