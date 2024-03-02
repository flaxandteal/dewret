import hashlib
import json
from collections.abc import Sequence, Mapping

BasicType = str | float | bool | bytes | int | None
RawType = BasicType | list["RawType"] | dict[str, "RawType"]


def is_raw(var):
    return isinstance(var, BasicType | list[RawType] | dict[str, RawType])

def hasher(construct: tuple | RawType) -> str:
    if isinstance(construct, Sequence) and not isinstance(construct, bytes | str):
        if isinstance(construct, Mapping):
            construct = tuple(construct.items())
        else:
            construct = tuple(construct)
    construct_as_string = json.dumps(construct)
    hsh = hashlib.md5()
    hsh.update(construct_as_string.encode())
    return hsh.hexdigest()

