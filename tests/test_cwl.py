"""Verify CWL output is OK."""

import yaml
from dewret.tasks import task, run
from dewret.renderers.cwl import render
from dewret.utils import hasher
from dewret.workflow import Workflow

@task()
def increment(num: int) -> int:
    """Increment an integer."""
    return num + 1

def test_cwl() -> None:
    """Check whether we can produce simple CWL.

    Produces simplest possible CWL from a workflow.
    """
    result = increment(num=3)
    workflow = run(result)
    rendered = render(workflow)
    hsh = hasher(('increment', ('num', 'int|3')))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        steps:
          increment-{hsh}:
            run: increment
            in:
                num:
                    default: 3
    """)
