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

"""Utility module.

General types and functions to centralize common logic.
"""

import hashlib
import json
import sys
import importlib
import importlib.util
import yaml
import re
from types import FrameType, TracebackType, UnionType, ModuleType
from typing import Any, cast, Protocol, ClassVar, Callable, Iterable, get_args, Hashable
from pathlib import Path
from collections.abc import Sequence, Mapping
from dataclasses import asdict, is_dataclass
from sympy import Basic, Integer, Float, Rational

from .core import (
    Reference,
    RawType,
    FirmType,
    Raw,
    RawRenderModule,
    StructuredRenderModule,
)


class Unset:
    """Unset variable, with no default value."""


class DataclassProtocol(Protocol):
    """Format of a dataclass.

    Since dataclasses do not expose a proper type, we use this to
    represent them.
    """

    __dataclass_fields__: ClassVar[dict[str, Any]]


def make_traceback(skip: int = 2) -> TracebackType | None:
    """Creates a traceback for the current frame.

    Necessary to allow tracebacks to be prepped for
    potential errors in lazy-evaluated functions.

    Args:
        skip: number of frames to skip before starting traceback.
    """
    frame: FrameType | None = sys._getframe(skip)
    tb = None
    while frame:
        tb = TracebackType(tb, frame, frame.f_lasti, frame.f_lineno)
        frame = frame.f_back
    return tb


def load_module_or_package(target_name: str, path: Path) -> ModuleType:
    """Convenience loader for modules.

    If an `__init__.py` is found in the same location as the target, it will try to load the renderer module
    as if it is contained in a package and, if it cannot, will fall back to loading the single file.

    Args:
        target_name: module name that should appear in `sys.modules`.
        path: location of the module.

    Returns: the loaded module.
    """
    module: None | ModuleType = None
    exception: None | Exception = None
    package_init = path.parent / "__init__.py"
    # Try to import the module as a package, if possible, to allow relative imports.
    if package_init.exists():
        try:
            spec = importlib.util.spec_from_file_location(
                target_name, str(package_init)
            )
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not open {path.parent} package")
            package = importlib.util.module_from_spec(spec)
            sys.modules[target_name] = package
            spec.loader.exec_module(package)
            module = importlib.import_module(f"{target_name}.{path.stem}", target_name)
        except ImportError as exc:
            exception = exc

    if module is None:
        try:
            spec = importlib.util.spec_from_file_location(target_name, str(path))
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not open {path} module")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except ImportError as exc:
            if exception:
                raise exc from exception
            raise exc

    return module


def flatten_if_set(value: Any) -> RawType | Unset:
    """Takes a Raw-like structure and makes it RawType or Unset.

    Flattens if the value is set, but otherwise returns the unset
    sentinel value as-is.

    Args:
        value: value to squash
    """
    if isinstance(value, Unset):
        return value
    return crawl_raw(value)


def crawl_raw(value: Any, action: Callable[[Any], Any] | None = None) -> RawType:
    """Takes a Raw-like structure and makes it RawType.

    Particularly useful for squashing any TypedDicts.

    Args:
        value: value to squash
        action: an callback to apply to each found entry, or None.

    Returns: a structuure that is guaranteed to be raw.

    Raises: RuntimeError if it cannot convert the value to raw.
    """
    if action is not None:
        value = action(value)

    if value is None:
        return value
    if isinstance(value, str) or isinstance(value, bytes):
        return value
    if isinstance(value, Mapping):
        return {key: crawl_raw(item, action) for key, item in value.items()}
    if is_dataclass(value) and not isinstance(value, type):
        return crawl_raw(asdict(value), action)
    if isinstance(value, Sequence):
        return [crawl_raw(item, action) for item in value]
    if (raw := ensure_raw(value)) is not None:
        return raw
    raise RuntimeError(f"Could not flatten: {value}")


def firm_to_raw(value: FirmType) -> RawType:
    """Convenience wrapper for firm structures.

    Turns structures that would be raw, except for tuples, into raw structures
    by mapping any tuples to lists.

    Args:
        value: a firm structure (contains raw/tuple values).

    Returns: a raw structure.
    """
    return crawl_raw(
        value, lambda entry: list(entry) if isinstance(entry, tuple) else entry
    )


def is_expr(value: Any, permitted_references: type = Reference) -> bool:
    """Confirms whether a structure has only raw or expression types.

    Args:
        value: a crawlable structure.
        permitted_references: a class representing the allowed types of References.

    Returns: True if valid, otherwise False.
    """
    return is_raw(
        value,
        lambda x: isinstance(x, Basic)
        or isinstance(x, tuple)
        or isinstance(x, permitted_references)
        or isinstance(x, Raw),
    )


def is_raw_type(typ: type) -> bool:
    """Check if a type counts as "raw"."""
    if isinstance(typ, UnionType):
        return all(is_raw_type(t) for t in get_args(typ))
    return issubclass(typ, str | float | bool | bytes | int | None | list | dict)


def is_firm(value: Any, check: Callable[[Any], bool] | None = None) -> bool:
    """Confirms whether a function is firm.

    That is, whether its contents are raw or tuples.

    Args:
        value: value to check.
        check: any additional check to apply.

    Returns: True if is firm, else False.
    """
    return is_raw(value, lambda x: isinstance(x, tuple) and (check is None or check(x)))


def is_raw(value: Any, check: Callable[[Any], bool] | None = None) -> bool:
    """Check if a variable counts as "raw".

    This works around a checking issue that isinstance of a union of types
    assigned to a variable, such as `RawType`, may throw errors even though Python 3.11+
    does not. Instead, we explicitly make the full union in the statement below.
    """
    # Ideally this would be:
    # isinstance(value, RawType | list[RawType] | dict[str, RawType])
    # but recursive types are problematic.
    if isinstance(
        value, str | float | bool | bytes | int | None | Integer | Float | Rational
    ):
        return True

    if check is not None and check(value):
        return True

    if isinstance(value, Mapping):
        return (
            (isinstance(value, dict) or (check is not None and check(value)))
            and all(is_raw(key, check) for key in value.keys())
            and all(is_raw(val, check) for val in value.values())
        )

    if isinstance(value, Iterable):
        return (
            isinstance(value, list) or (check is not None and check(value))
        ) and all(is_raw(key, check) for key in value)

    return False


def ensure_raw(value: Any, cast_tuple: bool = False) -> RawType | None:
    """Check if a variable counts as "raw".

    This works around a checking issue that isinstance of a union of types
    assigned to a variable, such as `RawType`, may throw errors even though Python 3.11+
    does not. Instead, we explicitly make the full union in the statement below.
    """
    # See is_raw:
    # isinstance(var, RawType | list[RawType] | dict[str, RawType])
    return cast(RawType, value) if is_raw(value) else None


def hasher(construct: FirmType) -> str:
    """Consistently hash a RawType or tuple structure.

    Turns a possibly nested structure of basic types, dicts, lists and tuples
    into a consistent hash.

    Args:
        construct: structure to hash.

    Returns:
        Hash string that should be unique to the construct. The limits of this uniqueness
        have not yet been explicitly calculated.
    """

    def _make_hashable(construct: FirmType) -> Hashable:
        hashed_construct: tuple[Hashable, ...]
        if isinstance(construct, Sequence) and not isinstance(construct, bytes | str):
            if isinstance(construct, Mapping):
                hashed_construct = tuple(
                    (k, hasher(v)) for k, v in sorted(construct.items())
                )
            else:
                # Cast to workaround recursive type
                hashed_construct = tuple(_make_hashable(v) for v in construct)
            return hashed_construct
        if not isinstance(construct, Hashable):
            raise TypeError("Could not hash arguments")
        return construct

    if isinstance(construct, Hashable):
        hashed_construct = construct
    else:
        hashed_construct = _make_hashable(construct)
    construct_as_string = json.dumps(hashed_construct)
    hsh = hashlib.md5()
    hsh.update(construct_as_string.encode())
    return hsh.hexdigest()


def format_user_args(args: str) -> dict[str, Any]:
    """Format user arguments from the command line.

    Supports:
    - @filename: loads arguments from a YAML file
    - empty string: returns an empty dict
    - key1:val1,key2:val2: parses a comma-separated list into a dict
    """
    kwargs: dict[str, Any]
    if args.startswith("@"):
        with Path(args[1:]).open() as construct_args_f:
            kwargs = yaml.safe_load(construct_args_f)
    elif not args:
        kwargs = {}
    else:
        kwargs = dict(pair.split(":") for pair in args.split(","))

    return kwargs


def get_json_args(args: list[str]) -> dict[str, Any]:
    """Parse a sequence of key:val strings into a dictionary, where values are JSON-parsed.

    Args:
        args: A sequence of strings, each in the format 'key:val', where `val` is a JSON literal.

    Returns:
        A dictionary mapping keys to their corresponding parsed JSON values.

    Raises:
        RuntimeError: If any argument is not in the expected 'key:val' format.
        json.JSONDecodeError: If a value is not valid JSON.
    """
    kwargs = {}
    for arg in args:
        if ":" not in arg:
            raise RuntimeError(
                "Arguments should be specified as key:val, where val is a JSON representation of the argument"
            )
        key, val = arg.split(":", 1)
        kwargs[key] = json.loads(val)
    return kwargs


def resolve_renderer(renderer: str) -> Path | RawRenderModule | StructuredRenderModule:
    """Resolve the renderer argument into either a module or a file path.

    If the renderer is a known name, attempts to import it as a module from `dewret.renderers`.
    If it starts with '@', treats the remainder as a file path.

    Args:
        renderer: The name of the renderer module or a file reference starting with '@'.

    Returns:
        A render module (imported) or a Path object to a renderer file.

    Raises:
        RuntimeError: If the renderer format is invalid.
        NotImplementedError: If the imported module does not conform to expected interfaces.
        ModuleNotFoundError: If the module cannot be imported.
    """
    render_module: Path | ModuleType
    if mtch := re.match(r"^([a-z_0-9-.]+)$", renderer):
        render_module = importlib.import_module(f"dewret.renderers.{mtch.group(1)}")
        if not isinstance(render_module, RawRenderModule) and not isinstance(
            render_module, StructuredRenderModule
        ):
            raise NotImplementedError(
                "The imported render module does not seem to match the `RawRenderModule` or `StructuredRenderModule` protocols."
            )
    elif renderer.startswith("@"):
        render_module = Path(renderer[1:])
    else:
        raise RuntimeError(
            "Renderer argument should be a known dewret renderer, or '@FILENAME' where FILENAME is a renderer"
        )

    return render_module
