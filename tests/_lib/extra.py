from dewret.tasks import task, subworkflow

JUMP: float = 1.0


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
    """Double an integer."""
    return num % 10


@task()
def sum(left: int | float, right: int | float) -> int | float:
    """Add two integers."""
    return left + right


@subworkflow()
def triple_and_one(num: int | float) -> int | float:
    """Triple a number by doubling and adding again, then add 1."""
    return sum(left=sum(left=double(num=num), right=num), right=1)


@task()
def tuple_float_return() -> tuple[float, float]:
    """Return a tuple of floats."""
    return 48.856667, 2.351667
