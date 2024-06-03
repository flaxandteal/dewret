"""Verify CWL output is OK."""

import yaml
from dewret.tasks import construct, task
from dewret.renderers.cwl import render
from dewret.utils import hasher
from dewret.workflow import param

from ._lib.extra import increment, double, mod10, sum, triple_and_one


@task()
def pi() -> float:
    """Returns pi."""
    import math

    return math.pi


@task()
def floor(num: int | float) -> int:
    """Converts int/float to int."""
    return int(num)


def test_basic_cwl() -> None:
    """Check whether we can produce simple CWL.

    Produces simplest possible CWL from a workflow, using
    a pure function.
    """
    result = pi()
    workflow = construct(result)
    rendered = render(workflow)
    hsh = hasher(("pi",))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs:
          out:
            label: out
            outputSource: pi-{hsh}/out
            type: double
        steps:
          pi-{hsh}:
            run: pi
            in: {{}}
            out: [out]
    """)


def test_cwl_with_parameter() -> None:
    """Check whether we can move raw input to parameters.

    Produces CWL for a call with a changeable raw value, that is converted
    to a parameter, if and only if we are calling from outside a nested task.
    """
    result = increment(num=3)
    workflow = construct(result)
    rendered = render(workflow)
    num_param = list(workflow.find_parameters())[0]
    hsh = hasher(("increment", ("num", f"int|:param:{num_param.unique_name}")))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-{hsh}-num:
            label: increment-{hsh}-num
            type: int
            default: 3
        outputs:
          out:
            label: out
            outputSource: increment-{hsh}/out
            type: int
        steps:
          increment-{hsh}:
            run: increment
            in:
                num:
                    source: increment-{hsh}-num
            out: [out]
    """)


def test_cwl_without_default() -> None:
    """Check whether we can produce CWL without a default value.

    Uses a manually created parameter to avoid a default.
    """
    my_param = param("my_param", typ=int)

    result = increment(num=my_param)
    workflow = construct(result)
    rendered = render(workflow)
    hsh = hasher(("increment", ("num", "int|:param:my_param")))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          my_param:
            label: my_param
            type: int
        outputs:
          out:
            label: out
            outputSource: increment-{hsh}/out
            type: int
        steps:
          increment-{hsh}:
            run: increment
            in:
                num:
                    source: my_param
            out: [out]
    """)


def test_cwl_with_subworkflow() -> None:
    """Check whether we can produce a subworkflow from CWL."""
    my_param = param("num", typ=int)
    result = increment(num=floor(num=triple_and_one(num=increment(num=my_param))))
    workflow = construct(result, simplify_ids=True)
    rendered, subworkflows = render(workflow)

    assert len(subworkflows) == 1
    assert isinstance(subworkflows, dict)
    name, subworkflow = list(subworkflows.items())[0]

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: int
        outputs:
          out:
            label: out
            outputSource: increment-2/out
            type: int
        steps:
          floor-1:
             in:
               num:
                 source: triple_and_one-1/out
             out: [out]
             run: floor
          increment-1:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          increment-2:
             in:
               num:
                 source: floor-1/out
             out: [out]
             run: increment
          triple_and_one-1:
             in:
               num:
                 source: increment-1/out
             out: [out]
             run: triple_and_one
    """)

    assert subworkflow == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: int
          sum-1-2-right:
            default: 1
            label: sum-1-2-right
            type: int
        outputs:
          out:
            label: out
            outputSource: sum-1-2/out
            type:
            - int
            - double
        steps:
          double-1-1:
            in:
              num:
                source: num
            out:
            - out
            run: double
          sum-1-1:
            in:
              left:
                source: double-1-1/out
              right:
                source: num
            out:
            - out
            run: sum
          sum-1-2:
            in:
              left:
                source: sum-1-1/out
              right:
                source: sum-1-2-right
            out:
            - out
            run: sum
    """)


def test_cwl_references() -> None:
    """Check whether we can link between steps.

    Produces CWL that has references between steps.
    """
    result = double(num=increment(num=3))
    workflow = construct(result)
    rendered = render(workflow)
    num_param = list(workflow.find_parameters())[0]
    hsh_increment = hasher(
        ("increment", ("num", f"int|:param:{num_param.unique_name}"))
    )
    hsh_double = hasher(("double", ("num", f"increment-{hsh_increment}/out")))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-{hsh_increment}-num:
            label: increment-{hsh_increment}-num
            type: int
            default: 3
        outputs:
          out:
            label: out
            outputSource: double-{hsh_double}/out
            type: [int, double]
        steps:
          increment-{hsh_increment}:
            run: increment
            in:
                num:
                    source: increment-{hsh_increment}-num
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
    result = sum(left=double(num=increment(num=23)), right=mod10(num=increment(num=23)))
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-1-num:
            label: increment-1-num
            type: int
            default: 23
          increment-2-num:
            label: increment-2-num
            type: int
            default: 23
        outputs:
          out:
            label: out
            outputSource: sum-1/out
            type: [int, double]
        steps:
          increment-1:
            run: increment
            in:
                num:
                    source: increment-1-num
            out: [out]
          increment-2:
            run: increment
            in:
                num:
                    source: increment-2-num
            out: [out]
          double-1:
            run: double
            in:
                num:
                    source: increment-2/out
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
