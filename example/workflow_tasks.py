"""Supporting tasks for complex workflow.

Illustrates the ability to modularize code.
"""

from dewret.tasks import task

JUMP: int = 10


@task()
def increase(num: int) -> int:
    """Add globally-configured integer JUMP to a number."""
    return num + JUMP


@task()
def increment(num: int) -> int:
    """Increment an integer."""
    return num + 1


@task()
def double(num: int) -> int:
    """Double an integer."""
    return 2 * num


@task()
def mod10(num: int) -> int:
    """Calculate supplied integer modulo 10."""
    return num % 10


@task()
def sum(left: int, right: int) -> int:
    """Add two integers."""
    return left + right
