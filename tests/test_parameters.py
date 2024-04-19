"""Verify CWL can be made with parameters."""

import yaml
from dewret.tasks import task, construct
from dewret.renderers.cwl import render

from ._lib.extra import double, sum

INPUT_NUM = 3

@task()
def rotate(num: int) -> int:
    """Rotate an integer."""
    return (num + INPUT_NUM) % INPUT_NUM


def test_cwl_parameters() -> None:
    """Check whether we can spot input parameters.

    Produces CWL that reference input parameters based on local/global variables.
    """
    result = rotate(num=3)
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          INPUT_NUM:
            label: INPUT_NUM
            type: int
        outputs:
          out:
            label: out
            outputSource: rotate-1/out
            type: int
        steps:
          rotate-1:
            run: rotate
            in:
                num:
                    default: 3
                INPUT_NUM:
                    source: INPUT_NUM
            out: [out]
    """)


def test_complex_parameters() -> None:
    """Check whether we can link between multiple steps and have parameters.

    Produces CWL that has references between multiple steps.
    """
    result = sum(
        left=double(num=rotate(num=23)),
        right=rotate(num=rotate(num=23))
    )
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          INPUT_NUM:
            label: INPUT_NUM
            type: int
        outputs:
          out:
            label: out
            outputSource: sum-1/out
            type: [int, double]
        steps:
          rotate-1:
            run: rotate
            in:
                INPUT_NUM:
                    source: INPUT_NUM
                num:
                    default: 23
            out: [out]
          double-1:
            run: double
            in:
                num:
                    source: rotate-1/out
            out: [out]
          rotate-2:
            run: rotate
            in:
                INPUT_NUM:
                    source: INPUT_NUM
                num:
                    source: rotate-1/out
            out: [out]
          sum-1:
            run: sum
            in:
                left:
                    source: double-1/out
                right:
                    source: rotate-2/out
            out: [out]
    """)
