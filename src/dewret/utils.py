import hashlib
import json
from typing import Any, cast
from collections.abc import Sequence

BasicType = str | float | bool | bytes | int | None
RawType = BasicType | list["RawType"] | dict[str, "RawType"]
FirmType = BasicType | list["FirmType"] | dict[str, "FirmType"] | tuple["FirmType", ...]


def is_raw(var: Any) -> bool:
    return isinstance(var, str | float | bool | bytes | int | None | list[RawType] | dict[str, RawType])

def hasher(construct: FirmType) -> str:
    if isinstance(construct, Sequence) and not isinstance(construct, bytes | str):
        if isinstance(construct, dict):
            construct = tuple((k, v) for k, v in construct.items())
        else:
            # Cast to workaround recursive type
            construct = cast(FirmType, tuple(construct))
    construct_as_string = json.dumps(construct)
    hsh = hashlib.md5()
    hsh.update(construct_as_string.encode())
    return hsh.hexdigest()

