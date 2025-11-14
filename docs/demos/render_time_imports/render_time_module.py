"""Module to demonstrate importing at render time."""


def caps(arg: str) -> str:
    """Convert a string to uppercase."""
    return arg.upper()


class MyStr:
    """A wrapper class meant to emulate the builtin upper metod function of strings."""

    def __init__(self, arg: str) -> None:
        """Initialize MyStr with a string value."""
        self.my_string: str = arg

    def upper(self) -> str:
        """Return the stored string converted to uppercase."""
        return self.my_string.upper()
