import inspect
import ast
import sys
import importlib
from functools import lru_cache
from types import FunctionType
from dataclasses import dataclass
from typing import Protocol, Any, TypeVar, Generic, cast, Literal, TypeAliasType, Annotated, Callable, get_origin, get_args, Mapping, TypeAliasType, get_type_hints

T = TypeVar("T")
AtRender = Annotated[T, "AtRender"]
Fixed = Annotated[T, "Fixed"]

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
        return get_type_hints(inspect.unwrap(self.fn), include_extras=True)["return"]

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

    @property
    def free_vars(self):
        if self.fn.__code__ and self.fn.__closure__:
            return dict(zip(self.fn.__code__.co_freevars, (c.cell_contents for c in self.fn.__closure__)))
        return {}

    def get_argument_annotation(self, arg: str, exhaustive: bool=False) -> type | None:
        all_annotations: dict[str, type] = {}
        typ: type | None = None
        if (typ := self.fn.__annotations__.get(arg)):
            if isinstance(typ, str):
                typ = get_type_hints(self.fn, include_extras=True).get(arg)
        elif exhaustive:
            if (anns := get_type_hints(sys.modules[self.fn.__module__], include_extras=True)):
                if (typ := anns.get(arg)):
                    ...
                elif (orig_pair := self.get_all_imported_names().get(arg)):
                    orig_module, orig_name = orig_pair
                    typ = orig_module.__annotations__.get(orig_name)
                elif (value := self.free_vars.get(arg)):
                    if not inspect.isclass(value) or inspect.isfunction(value):
                        raise RuntimeError(f"Cannot use free variables - please put {arg} at the global scope")
        return typ

    def argument_has(self, arg: str, annotation: type, exhaustive: bool=False) -> bool:
        typ = self.get_argument_annotation(arg, exhaustive)
        return bool(typ and self._typ_has(typ, annotation))

    def is_at_construct_arg(self, arg: str, exhaustive: bool=False) -> bool:
        return self.argument_has(arg, AtRender, exhaustive)

    @property
    def globals(self) -> Mapping[str, Any]:
        try:
            fn_tuple = inspect.getclosurevars(self.fn)
            fn_globals = dict(fn_tuple.globals)
            fn_globals.update(fn_tuple.nonlocals)
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
