from dewret.tasks import task, run

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

