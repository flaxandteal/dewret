from attrs import define
from dewret.tasks import task, workflow

from .other import nothing

JUMP: float = 1.0
test: float = nothing


@define
class PackResult:
    """A class representing the counts of card suits in a deck, including hearts, clubs, spades, and diamonds."""

    hearts: int
    clubs: int
    spades: int
    diamonds: int

@workflow()
def try_nothing() -> int:
    """Check that we can see AtRender in another module."""
    if nothing:
        return increment(num=1)
    return increment(num=0)


@task()
def increase(num: int | float) -> float:
    """Add 1 to a number."""
    return num + JUMP


@task()
def increment(num: int) -> int:
    """Increment an integer."""
    return num + 1


@task()
def double(num: int | float) -> int | float:
    """Double an integer."""
    return 2 * num


@task()
def mod10(num: int) -> int:
    """Remainder of an integer divided by 10."""
    return num % 10


@task()
def sum(left: int | float, right: int | float) -> int | float:
    """Add two integers."""
    return left + right


@task()
def pi() -> float:
    """Returns pi."""
    import math

    return math.pi


@workflow()
def triple_and_one(num: int | float) -> int | float:
    """Triple a number by doubling and adding again, then add 1."""
    return sum(left=sum(left=double(num=num), right=num), right=1)


@task()
def tuple_float_return() -> tuple[float, float]:
    """Return a tuple of floats."""
    return 48.856667, 2.351667


@task()
def reverse_list(to_sort: list[int | float]) -> list[int | float]:
    return to_sort[::-1]


@task()
def max_list(lst: list[int | float]) -> int | float:
    return max(lst)


