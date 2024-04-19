"""Verify CWL can be made with split up and nested calls."""

import yaml
from dewret.tasks import nested_task, construct
from dewret.renderers.cwl import render
from ._lib.extra import double, sum, increase

STARTING_NUMBER: int = 23

@nested_task()
def algorithm() -> int | float:
    """Creates a graph of task calls."""
    left = double(num=increase(num=STARTING_NUMBER))
    right = increase(num=increase(num=17))
    return sum(
        left=left,
        right=right
    )

def test_nested_task() -> None:
    """Check whether we can link between multiple steps and have parameters.

    Produces CWL that has references between multiple steps.
    """
    workflow = construct(algorithm(), simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          JUMP:
            label: JUMP
            type: double
        outputs:
          out:
            label: out
            outputSource: sum-1/out
            type: [int, double]
        steps:
          increase-1:
            run: increase
            in:
                JUMP:
                    source: JUMP
                num:
                    default: 17
            out: [out]
          increase-2:
            run: increase
            in:
                JUMP:
                    source: JUMP
                num:
                    source: increase-1/out
            out: [out]
          increase-3:
            run: increase
            in:
                JUMP:
                    source: JUMP
                num:
                    default: 23
            out: [out]
          double-1:
            run: double
            in:
                num:
                    source: increase-3/out
            out: [out]
          sum-1:
            run: sum
            in:
                left:
                    source: double-1/out
                right:
                    source: increase-2/out
            out: [out]
    """)
