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
from types import FrameType, TracebackType
from typing import Any, cast, Union, Protocol, ClassVar
from collections.abc import Sequence, Mapping

BasicType = str | float | bool | bytes | int | None
RawType = Union[BasicType, list["RawType"], dict[str, "RawType"]]
FirmType = BasicType | list["FirmType"] | dict[str, "FirmType"] | tuple["FirmType", ...]


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


def flatten(value: Any) -> RawType:
    """Takes a Raw-like structure and makes it RawType.

    Particularly useful for squashing any TypedDicts.

    Args:
        value: value to squash
    """
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


def is_raw(value: Any) -> bool:
    """Check if a variable counts as "raw".

    This works around a checking issue that isinstance of a union of types
    assigned to a variable, such as `RawType`, may throw errors even though Python 3.11+
    does not. Instead, we explicitly make the full union in the statement below.
    """
    # Ideally this would be:
    # isinstance(value, RawType | list[RawType] | dict[str, RawType])
    # but recursive types are problematic.
    return isinstance(value, str | float | bool | bytes | int | None | list | dict)


def ensure_raw(value: Any) -> RawType | None:
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
        if isinstance(construct, dict):
            construct = list([k, hasher(v)] for k, v in sorted(construct.items()))
        else:
            # Cast to workaround recursive type
            construct = cast(FirmType, list(construct))
    construct_as_string = json.dumps(construct)
    hsh = hashlib.md5()
    hsh.update(construct_as_string.encode())
    return hsh.hexdigest()
