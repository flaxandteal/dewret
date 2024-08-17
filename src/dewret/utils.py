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
from types import FrameType, TracebackType, UnionType
from typing import Any, cast, Union, Protocol, ClassVar, Callable, Iterable, get_args, get_origin, Annotated
from collections.abc import Sequence, Mapping
from sympy import Basic, Integer, Float, Rational

from .core import Reference, BasicType, RawType, FirmType, Raw


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


def flatten_if_set(value: Any) -> RawType | Unset:
    """Takes a Raw-like structure and makes it RawType or Unset.

    Flattens if the value is set, but otherwise returns the unset
    sentinel value as-is.

    Args:
        value: value to squash
    """
    if isinstance(value, Unset):
        return value
    return flatten(value)

def crawl_raw(value: Any, action: Callable[[Any], Any]) -> RawType:
    """Takes a Raw-like structure and makes it RawType.

    Particularly useful for squashing any TypedDicts.

    Args:
        value: value to squash
    """

    value = action(value)

    if value is None:
        return value
    if isinstance(value, str) or isinstance(value, bytes):
        return value
    if isinstance(value, Mapping):
        return {key: flatten(item) for key, item in value.items()}
    if isinstance(value, Sequence):
        return [flatten(item) for item in value]
    if (raw := ensure_raw(value)) is not None:
        return raw
    raise RuntimeError(f"Could not flatten: {value}")

def firm_to_raw(value: FirmType) -> RawType:
    return crawl_raw(value, lambda entry: list(entry) if isinstance(entry, tuple) else entry)

def flatten(value: Any) -> RawType:
    return crawl_raw(value, lambda entry: entry)

def is_expr(value: Any) -> bool:
    return is_raw(value, lambda x: isinstance(x, Basic) or isinstance(x, tuple) or isinstance(x, Reference) or isinstance(x, Raw))

def strip_annotations(parent_type: type) -> tuple[type, tuple]:
    # Strip out any annotations. This should be auto-flattened, so in theory only one iteration could occur.
    metadata = []
    while get_origin(parent_type) is Annotated:
        parent_type, *parent_metadata = get_args(parent_type)
        metadata += list(parent_metadata)
    return parent_type, tuple(metadata)

def is_raw_type(typ: type) -> bool:
    """Check if a type counts as "raw"."""
    if isinstance(typ, UnionType):
        return all(is_raw_type(t) for t in get_args(typ))
    return issubclass(typ, str | float | bool | bytes | int | None | list | dict)


def is_raw(value: Any, check: Callable[[Any], bool] | None = None) -> bool:
    """Check if a variable counts as "raw".

    This works around a checking issue that isinstance of a union of types
    assigned to a variable, such as `RawType`, may throw errors even though Python 3.11+
    does not. Instead, we explicitly make the full union in the statement below.
    """
    # Ideally this would be:
    # isinstance(value, RawType | list[RawType] | dict[str, RawType])
    # but recursive types are problematic.
    if isinstance(value, str | float | bool | bytes | int | None | Integer | Float | Rational):
        return True

    if check is not None and check(value):
        return True

    if isinstance(value, Mapping):
        return (
            (isinstance(value, dict) or (check is not None and check(value))) and
            all(is_raw(key, check) for key in value.keys()) and
            all(is_raw(val, check) for val in value.values())
        )

    if isinstance(value, Iterable):
        return (
            (isinstance(value, list) or (check is not None and check(value))) and
            all(is_raw(key, check) for key in value)
        )

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
    if isinstance(construct, Sequence) and not isinstance(construct, bytes | str):
        if isinstance(construct, Mapping):
            construct = list([k, hasher(v)] for k, v in sorted(construct.items()))
        else:
            # Cast to workaround recursive type
            construct = cast(FirmType, list(construct))
    construct_as_string = json.dumps(construct)
    hsh = hashlib.md5()
    hsh.update(construct_as_string.encode())
    return hsh.hexdigest()
