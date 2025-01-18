"""Verify CWL can be made with parameters."""

import yaml
from dewret.tasks import task, construct
from dewret.workflow import param
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
    rendered = render(workflow)["__root__"]

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          INPUT_NUM:
            label: INPUT_NUM
            type: int
            default: 3
          rotate-1-num:
            label: num
            type: int
            default: 3
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
                    source: rotate-1-num
                INPUT_NUM:
                    source: INPUT_NUM
            out: [out]
    """)


def test_complex_parameters() -> None:
    """Check whether we can link between multiple steps and have parameters.

    Produces CWL that has references between multiple steps.
    """
    num = param("numx", 23)
    result = sum(left=double(num=rotate(num=num)), right=rotate(num=rotate(num=23)))
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          INPUT_NUM:
            label: INPUT_NUM
            type: int
            default: 3
          numx:
            label: numx
            type: int
            default: 23
          rotate-2-num:
            label: num
            type: int
            default: 23
        outputs:
          out:
            label: out
            outputSource: sum-1/out
            type: [int, float]
        steps:
          rotate-1:
            run: rotate
            in:
                INPUT_NUM:
                    source: INPUT_NUM
                num:
                    source: rotate-2/out
            out: [out]
          double-1:
            run: double
            in:
                num:
                    source: rotate-3/out
            out: [out]
          rotate-2:
            run: rotate
            in:
                INPUT_NUM:
                    source: INPUT_NUM
                num:
                    source: rotate-2-num
            out: [out]
          rotate-3:
            run: rotate
            in:
                INPUT_NUM:
                    source: INPUT_NUM
                num:
                    source: numx
            out: [out]
          sum-1:
            run: sum
            in:
                left:
                    source: double-1/out
                right:
                    source: rotate-1/out
            out: [out]
    """)
