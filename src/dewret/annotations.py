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

"""Tooling for managing annotations.

Provides `FunctionAnalyser`, a toolkit that takes a `Callable` and can interrogate it
for annotations, with some intelligent searching beyond the obvious location.
"""

import inspect
import ast
import sys
import importlib
from ._cpython_mod import getclosurevars
from functools import lru_cache
from types import FunctionType, ModuleType
from typing import (
    Any,
    TypeVar,
    Annotated,
    Callable,
    get_origin,
    get_args,
    Mapping,
    get_type_hints,
)

T = TypeVar("T")
AtRender = Annotated[T, "AtRender"]
Fixed = Annotated[T, "Fixed"]


class FunctionAnalyser:
    """Convenience class for analysing a function with reduced duplication of effort.

    Attributes:
        _fn: the wrapped callable
        _annotations: stored annotations for the function.
    """

    _fn: Callable[..., Any]
    _annotations: dict[str, Any]

    def __init__(self, fn: Callable[..., Any]):
        """Set the function.

        If `fn` is a class, it takes the constructor, and if it is a method, it takes
        the `__func__` attribute.
        """
        if inspect.isclass(fn):
            self.fn = fn.__init__
        elif inspect.ismethod(fn):
            self.fn = fn.__func__
        else:
            self.fn = fn

    @property
    def return_type(self) -> Any:
        """Return type of the callable.

        Returns: expected type of the return value.

        Raises:
          ValueError: if the return value does not appear to be type-hinted.
        """
        hints = get_type_hints(inspect.unwrap(self.fn), include_extras=True)
        if "return" not in hints or hints["return"] is None:
            raise ValueError(f"Could not find type-hint for return value of {self.fn}")
        typ = hints["return"]
        return typ

    @staticmethod
    def _typ_has(typ: type, annotation: type) -> bool:
        """Check if the type has an annotation.

        Args:
            typ: type to check.
            annotation: the Annotated to compare against.

        Returns: True if the type has the given annotation, otherwise False.
        """
        if not hasattr(annotation, "__metadata__"):
            return False
        if origin := get_origin(typ):
            if (
                origin is Annotated
                and hasattr(typ, "__metadata__")
                and typ.__metadata__ == annotation.__metadata__
            ):
                return True
        if any(FunctionAnalyser._typ_has(arg, annotation) for arg in get_args(typ)):
            return True
        return False

    def get_all_module_names(self) -> dict[str, Any]:
        """Find all of the annotations within this module."""
        return get_type_hints(sys.modules[self.fn.__module__], include_extras=True)

    def get_all_imported_names(self) -> dict[str, tuple[ModuleType, str]]:
        """Find all of the annotations that were imported into this module."""
        return self._get_all_imported_names(sys.modules[self.fn.__module__])

    @staticmethod
    @lru_cache
    def _get_all_imported_names(mod: ModuleType) -> dict[str, tuple[ModuleType, str]]:
        """Get all of the names with this module, and their original locations.

        Args:
            mod: a module in the `sys.modules`.

        Returns:
            A dict whose keys are the known names in the current module, where the Callable lives,
            and whose values are pairs of the module and the remote name.
        """
        ast_tree = ast.parse(inspect.getsource(mod))
        imported_names = {}
        for node in ast.walk(ast_tree):
            if isinstance(node, ast.ImportFrom):
                for name in node.names:
                    imported_names[name.asname or name.name] = (
                        importlib.import_module(
                            "".join(["."] * node.level) + (node.module or ""),
                            package=mod.__package__,
                        ),
                        name.name,
                    )
        return imported_names

    @property
    def free_vars(self) -> dict[str, Any]:
        """Get the free variables for this Callable."""
        if self.fn.__code__ and self.fn.__closure__:
            return dict(
                zip(
                    self.fn.__code__.co_freevars,
                    (c.cell_contents for c in self.fn.__closure__),
                    strict=False,
                )
            )
        return {}

    def get_argument_annotation(self, arg: str, exhaustive: bool = False) -> Any:
        """Retrieve the annotations for this argument.

        Args:
            arg: name of the argument.
            exhaustive: True if we should search outside the function itself, into the module globals, and into imported modules.

        Returns: annotation if available, else None.
        """
        typ: type | None = None
        if typ := self.fn.__annotations__.get(arg):
            if isinstance(typ, str):
                typ = get_type_hints(self.fn, include_extras=True).get(arg)
        elif exhaustive:
            if anns := get_type_hints(
                sys.modules[self.fn.__module__], include_extras=True
            ):
                if typ := anns.get(arg):
                    ...
                elif orig_pair := self.get_all_imported_names().get(arg):
                    orig_module, orig_name = orig_pair
                    typ = orig_module.__annotations__.get(orig_name)
                elif value := self.free_vars.get(arg):
                    if not inspect.isclass(value) or inspect.isfunction(value):
                        raise RuntimeError(
                            f"Cannot use free variables - please put {arg} at the global scope"
                        )
        return typ

    def argument_has(
        self, arg: str, annotation: type, exhaustive: bool = False
    ) -> bool:
        """Check if the named argument has the given annotation.

        Args:
            arg: argument to retrieve.
            annotation: Annotated to search for.
            exhaustive: whether to check the globals and other modules.

        Returns: True if the Annotated is present in this argument's annotation.
        """
        typ = self.get_argument_annotation(arg, exhaustive)
        return bool(typ and self._typ_has(typ, annotation))

    def is_at_construct_arg(self, arg: str, exhaustive: bool = False) -> bool:
        """Convience function to check for `AtConstruct`, wrapping `FunctionAnalyser.argument_has`."""
        return self.argument_has(arg, AtRender, exhaustive)

    @property
    def globals(self) -> Mapping[str, Any]:
        """Get the globals for this Callable."""
        try:
            fn_tuple = getclosurevars(self.fn)
            fn_globals = dict(fn_tuple.globals)
            fn_globals.update(fn_tuple.nonlocals)
        # This covers the case of wrapping, rather than decorating.
        except TypeError:
            fn_globals = {}
        return fn_globals

    @property
    def unbound(self) -> dict[str, str | None]:
        """Get the globals for this Callable."""
        fn_tuple = getclosurevars(self.fn)
        unbound: dict[str, str | None] = {}
        for var in fn_tuple.unbound:
            if ":" in var:
                ref, impt = var.split(":")
                unbound[ref] = impt
            else:
                unbound[ref] = None
        return unbound

    def with_new_globals(self, new_globals: dict[str, Any]) -> Callable[..., Any]:
        """Create a Callable that will run the current Callable with new globals."""
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
