"""Example workflow file."""

from dewret.tasks import task


@task()
def my_task(input: str) -> str:
    """Example task."""
    return f"Processed: {input}"
