import pytest
import yaml
from typing import Literal

from dewret.tasks import task, construct, workflow, TaskException
from dewret.renderers.cwl import render
from dewret.annotations import AtRender, FunctionAnalyser, Fixed
from dewret.core import set_configuration

from ._lib.extra import increment, sum, try_nothing

ARG1: AtRender[bool] = True
ARG2: bool = False

class MyClass:
    def method(self, arg1: bool, arg2: AtRender[int]) -> float:
        arg3: float = 7.0
        arg4: AtRender[float] = 8.0
        return arg1 + arg2 + arg3 + arg4 + int(ARG1) + int(ARG2)

def fn(arg5: int, arg6: AtRender[int]) -> float:
    arg7: float = 7.0
    arg8: AtRender[float] = 8.0
    return arg5 + arg6 + arg7 + arg8 + int(ARG1) + int(ARG2)


@workflow()
def to_int_bad(num: int, should_double: bool) -> int | float:
    """Cast to an int."""
    return increment(num=num) if should_double else sum(left=num, right=num)

@workflow()
def to_int(num: int, should_double: AtRender[bool]) -> int | float:
    """Cast to an int."""
    return increment(num=num) if should_double else sum(left=num, right=num)

def test_can_analyze_annotations():
    my_obj = MyClass()

    analyser = FunctionAnalyser(my_obj.method)
    assert analyser.argument_has("arg1", AtRender, exhaustive=True) is False
    assert analyser.argument_has("arg3", AtRender, exhaustive=True) is False
    assert analyser.argument_has("ARG2", AtRender, exhaustive=True) is False
    assert analyser.argument_has("arg2", AtRender, exhaustive=True) is True
    assert analyser.argument_has("arg4", AtRender, exhaustive=True) is False # Not a global/argument
    assert analyser.argument_has("ARG1", AtRender, exhaustive=True) is True
    assert analyser.argument_has("ARG1", AtRender) is False

    analyser = FunctionAnalyser(fn)
    assert analyser.argument_has("arg5", AtRender, exhaustive=True) is False
    assert analyser.argument_has("arg7", AtRender, exhaustive=True) is False
    assert analyser.argument_has("ARG2", AtRender, exhaustive=True) is False
    assert analyser.argument_has("arg6", AtRender, exhaustive=True) is True
    assert analyser.argument_has("arg8", AtRender, exhaustive=True) is False # Not a global/argument
    assert analyser.argument_has("ARG1", AtRender, exhaustive=True) is True
    assert analyser.argument_has("ARG1", AtRender) is False

def test_at_render() -> None:
    with pytest.raises(TaskException) as _:
        result = to_int_bad(num=increment(num=3), should_double=True)
        wkflw = construct(result, simplify_ids=True)

    result = to_int(num=increment(num=3), should_double=True)
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw, allow_complex_types=True)
    rendered = subworkflows["__root__"]
    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-1-num:
            default: 3
            label: num
            type: int
        outputs:
          out:
            label: out
            outputSource: to_int-1/out
            type:
            - int
            - float
        steps:
          increment-1:
            in:
              num:
                source: increment-1-num
            out:
            - out
            run: increment
          to_int-1:
            in:
              num:
                source: increment-1/out
              should_double:
                default: True
            out:
            - out
            run: to_int
    """)

    result = to_int(num=increment(num=3), should_double=False)
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw, allow_complex_types=True)
    rendered = subworkflows["__root__"]
    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-1-num:
            default: 3
            label: num
            type: int
        outputs:
          out:
            label: out
            outputSource: to_int-1/out
            type:
            - int
            - float
        steps:
          increment-1:
            in:
              num:
                source: increment-1-num
            out:
            - out
            run: increment
          to_int-1:
            in:
              num:
                source: increment-1/out
              should_double:
                default: False
            out:
            - out
            run: to_int
    """)


def test_at_render_between_modules() -> None:
    nothing = False
    result = try_nothing()
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw, allow_complex_types=True)
    rendered = subworkflows["__root__"]

list_2: Fixed[list[int]] = [0, 1, 2, 3]

def test_can_loop_over_fixed_length() -> None:
    @workflow()
    def loop_over_lists(list_1: list[int]) -> list[int]:
        result = []
        for a, b in zip(list_1, list_2):
            result.append(a + b + len(list_2))
        return result

    with set_configuration(flatten_all_nested=True):
        result = loop_over_lists(list_1=[5, 6, 7, 8])
        wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw, allow_complex_types=True)
    rendered = subworkflows["__root__"]
    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          expression: '[4 + list_1[0] + list_2[0], 4 + list_1[1] + list_2[1], 4 + list_1[2] + list_2[2],
            4 + list_1[3] + list_2[3]]'
          source:
          - list_1
          - list_2
        steps: {}
    """)
