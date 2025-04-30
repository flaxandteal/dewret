from typing import IO, cast
from pathlib import Path
from os import PathLike


class Dataset: ...


class DatasetPath(Dataset, Path):
    def __truediv__(self, other: PathLike[str] | str) -> Path:
        return cast(
            Path, super().__truediv__(other)
        )  # Cast this up to make sure mypy flags abuse of this


class DataManager:
    def path(self, mode: str = "r") -> DatasetPath:
        # Idea is that this can be opened by the decorator.
        return DatasetPath("/tmp/test")
        # return cast(DatasetPath, NamedTemporaryFile(mode, delete=True, delete_on_close=False))

    def io(self, mode: str = "r") -> IO[str] | IO[bytes]:
        # Idea is that this can be opened by the decorator.
        # This should be able to apply logic to the mode, given it is a literal, to get typehinting
        # one or other of the union.
        return cast(IO[str] | IO[bytes], Dataset())
        # return cast(IO[str] | IO[bytes], NamedTemporaryFile(mode, delete=True, delete_on_close=False))
