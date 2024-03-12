"""Quickstart workflow.

Useful as an example of a simple workflow.
"""

from dewret.tasks import task

@task()
def increment(num: int) -> int:
    """Add 1 to a number."""
    return num + 1
