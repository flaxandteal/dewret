"""Check subworkflow behaviour is as expected."""

import yaml
from dewret.tasks import construct, subworkflow, task
from dewret.renderers.cwl import render
from dewret.workflow import param

from ._lib.extra import increment, sum

CONSTANT = 3


@task()
def to_int(num: int | float) -> int:
    """Cast to an int."""
    return int(num)


@subworkflow()
def add_constant(num: int | float) -> int:
    """Add a global constant to a number."""
    return to_int(num=sum(left=num, right=CONSTANT))


def test_subworkflows_can_use_globals() -> None:
    """Check whether we can produce a subworkflow from CWL."""
    my_param = param("num", typ=int)
    result = increment(num=add_constant(num=increment(num=my_param)))
    workflow = construct(result, simplify_ids=True)
    rendered, subworkflows = render(workflow)

    assert len(subworkflows) == 1
    assert isinstance(subworkflows, dict)

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          CONSTANT:
            label: CONSTANT
            default: 3
            type: int
          num:
            label: num
            type: int
        outputs:
          out:
            label: out
            outputSource: increment-2/out
            type: int
        steps:
          increment-1:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          increment-2:
             in:
               num:
                 source: add_constant-1/out
             out: [out]
             run: increment
          add_constant-1:
             in:
               CONSTANT:
                 source: CONSTANT
               num:
                 source: increment-1/out
             out: [out]
             run: add_constant
    """)
