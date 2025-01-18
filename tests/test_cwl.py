"""Verify CWL output is OK."""

import yaml
import pytest
from datetime import datetime, timedelta
from dewret.core import set_configuration
from dewret.tasks import construct, task, factory, TaskException
from dewret.renderers.cwl import render
from dewret.utils import hasher
from dewret.workflow import param

from ._lib.extra import (
    pi,
    increment,
    double,
    mod10,
    sum,
    triple_and_one,
    tuple_float_return,
)


@task()
def floor(num: int | float) -> int:
    """Converts int/float to int."""
    return int(num)


@task()
def days_in_future(now: datetime, num: int | float) -> datetime:
    """Add `num` days to `now`.

    Args:
        now: current datetime.
        num: count of days.
    """
    return now + timedelta(days=num)


def test_basic_cwl() -> None:
    """Check whether we can produce simple CWL.

    Produces simplest possible CWL from a workflow, using
    a pure function.
    """
    result = pi()
    workflow = construct(result)
    rendered = render(workflow)["__root__"]
    hsh = hasher(("pi",))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs:
          out:
            label: out
            outputSource: pi-{hsh}/out
            type: float
        steps:
          pi-{hsh}:
            run: pi
            in: {{}}
            out: [out]
    """)


def test_input_factories() -> None:
    """Use input factories to input complex types.

    Tests whether input factories can be treated as steps,
    or complex input, as a flag choice to the renderer.
    """

    def get_now() -> datetime:
        return datetime.now()

    now = factory(get_now)()
    result = days_in_future(now=now, num=3)
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow, allow_complex_types=True, factories_as_params=True)[
        "__root__"
    ]

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          days_in_future-1-num:
            default: 3
            type: int
            label: num
          get_now-1:
            label: get_now-1
            type: datetime
        outputs:
          out:
            label: out
            outputSource: days_in_future-1/out
            type: datetime
        steps:
          days_in_future-1:
            run: days_in_future
            in:
              num:
                source: days_in_future-1-num
              now:
                source: get_now-1/out
            out: [out]
    """)

    rendered = render(workflow, allow_complex_types=True)["__root__"]

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          days_in_future-1-num:
            default: 3
            type: int
            label: num
        outputs:
          out:
            label: out
            outputSource: days_in_future-1/out
            type: datetime
        steps:
          days_in_future-1:
            run: days_in_future
            in:
              num:
                source: days_in_future-1-num
              now:
                source: get_now-1/out
            out: [out]
          get_now-1:
            in: {}
            out: [out]
            run: get_now
    """)


def test_cwl_with_parameter() -> None:
    """Check whether we can move raw input to parameters.

    Produces CWL for a call with a changeable raw value, that is converted
    to a parameter, if and only if we are calling from outside a subworkflow.
    """
    result = increment(num=3)
    workflow = construct(result)
    rendered = render(workflow)["__root__"]
    num_param = list(workflow.find_parameters())[0]
    hsh = hasher(("increment", ("num", f"int|:param:{num_param._.unique_name}")))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-{hsh}-num:
            label: num
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


def test_cwl_with_positional_parameter() -> None:
    """Check whether we can move raw input to parameters.

    Produces CWL for a call with a changeable raw value, that is converted
    to a parameter, if and only if we are calling from outside a subworkflow.
    """
    with pytest.raises(TaskException) as _:
        result = increment(3)
    with set_configuration(allow_positional_args=True):
        result = increment(3)
        workflow = construct(result)
        rendered = render(workflow)["__root__"]
    num_param = list(workflow.find_parameters())[0]
    hsh = hasher(("increment", ("num", f"int|:param:{num_param._.unique_name}")))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-{hsh}-num:
            label: num
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
    rendered = render(workflow)["__root__"]
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
    subworkflows = render(workflow)
    rendered = subworkflows["__root__"]
    del subworkflows["__root__"]

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
            outputSource: increment-1/out
            type: int
        steps:
          floor-1:
             in:
               num:
                 source: triple_and_one-1/out
             out: [out]
             run: floor
          increment-2:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          increment-1:
             in:
               num:
                 source: floor-1/out
             out: [out]
             run: increment
          triple_and_one-1:
             in:
               num:
                 source: increment-2/out
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
        outputs:
          out:
            label: out
            outputSource: sum-1-1/out
            type:
            - int
            - float
        steps:
          double-1-1:
            in:
              num:
                source: num
            out:
            - out
            run: double
          sum-1-2:
            in:
              left:
                source: double-1-1/out
              right:
                source: num
            out:
            - out
            run: sum
          sum-1-1:
            in:
              left:
                source: sum-1-2/out
              right:
                default: 1
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
    rendered = render(workflow)["__root__"]
    num_param = list(workflow.find_parameters())[0]
    hsh_increment = hasher(
        ("increment", ("num", f"int|:param:{num_param._.unique_name}"))
    )
    hsh_double = hasher(("double", ("num", f"increment-{hsh_increment}")))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-{hsh_increment}-num:
            label: num
            type: int
            default: 3
        outputs:
          out:
            label: out
            outputSource: double-{hsh_double}/out
            type:
            - int
            - float
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
    rendered = render(workflow)["__root__"]

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-1-num:
            label: num
            type: int
            default: 23
        outputs:
          out:
            label: out
            outputSource: sum-1/out
            type:
            - int
            - float
        steps:
          increment-1:
            run: increment
            in:
                num:
                    source: increment-1-num
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


def test_cwl_with_subworkflow_and_raw_params() -> None:
    """Check whether we can produce a subworkflow from CWL."""
    my_param = param("num", typ=int)
    result = increment(num=floor(num=triple_and_one(num=sum(left=my_param, right=3))))
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow)
    rendered = subworkflows["__root__"]

    del subworkflows["__root__"]
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
          sum-1-right:
            default: 3
            label: right
            type: int
        outputs:
          out:
            label: out
            outputSource: increment-1/out
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
                 source: floor-1/out
             out: [out]
             run: increment
          sum-1:
             in:
               left:
                 source: num
               right:
                 source: sum-1-right
             out: [out]
             run: sum
          triple_and_one-1:
             in:
               num:
                 source: sum-1/out
             out: [out]
             run: triple_and_one
    """)

    assert subworkflow == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: [
              int,
              float
            ]
        outputs:
          out:
            label: out
            outputSource: sum-1-1/out
            type:
            - int
            - float
        steps:
          double-1-1:
            in:
              num:
                source: num
            out:
            - out
            run: double
          sum-1-2:
            in:
              left:
                source: double-1-1/out
              right:
                source: num
            out:
            - out
            run: sum
          sum-1-1:
            in:
              left:
                source: sum-1-2/out
              right:
                default: 1
            out:
            - out
            run: sum
    """)


def test_tuple_floats() -> None:
    """Checks whether we can return a tuple.

    Produces CWL that has a tuple of 2 values of type float.
    """
    result = tuple_float_return()
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]
    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs: {}
        outputs:
          out:
            label: out
            outputSource: tuple_float_return-1/out
            items:
              - float
              - float
            type: array
        steps:
          tuple_float_return-1:
            run: tuple_float_return
            in: {}
            out: [out]
    """)
