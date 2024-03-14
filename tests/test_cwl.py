"""Verify CWL output is OK."""

import yaml
from dewret.tasks import task, run
from dewret.renderers.cwl import render
from dewret.utils import hasher
from dewret.workflow import Workflow

from ._lib.extra import (
    increment,
    double,
    mod10,
    sum
)

def test_cwl() -> None:
    """Check whether we can produce simple CWL.

    Produces simplest possible CWL from a workflow.
    """
    result = increment(num=3)
    workflow = run(result)
    rendered = render(workflow)
    hsh = hasher(("increment", ("num", "int|3")))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs:
          out:
            outputSource: increment-{hsh}/out
            type: int
        steps:
          increment-{hsh}:
            run: increment
            in:
                num:
                    default: 3
            out: [out]
    """)

def test_cwl_references() -> None:
    """Check whether we can link between steps.

    Produces CWL that has references between steps.
    """
    result = double(num=increment(num=3))
    workflow = run(result)
    rendered = render(workflow)
    hsh_increment = hasher(("increment", ("num", "int|3")))
    hsh_double = hasher(("double", ("num", f"increment-{hsh_increment}/out")))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs:
          out:
            outputSource: double-{hsh_double}/out
            type: [int, double]
        steps:
          increment-{hsh_increment}:
            run: increment
            in:
                num:
                    default: 3
            out: [out]
          double-{hsh_double}:
            run: double
            in:
                num:
                    source: increment-{hsh_increment}/out
            out: [out]
    """)

def test_complex_cwl_references() -> None:
    """Check whether we can link between multiple steps.

    Produces CWL that has references between multiple steps.
    """
    result = sum(
        left=double(num=increment(num=23)),
        right=mod10(num=increment(num=23))
    )
    workflow = run(result, simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs:
          out:
            outputSource: sum-1/out
            type: [int, double]
        steps:
          increment-1:
            run: increment
            in:
                num:
                    default: 23
            out: [out]
          double-1:
            run: double
            in:
                num:
                    source: increment-1/out
            out: [out]
          mod10-1:
            run: mod10
            in:
                num:
                    source: increment-1/out
            out: [out]
          sum-1:
            run: sum
            in:
                left:
                    source: double-1/out
                right:
                    source: mod10-1/out
            out: [out]
    """)
