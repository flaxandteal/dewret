import pytest
import yaml

from dewret.tasks import task, construct, subworkflow, TaskException
from dewret.renderers.cwl import render
from dewret.annotations import AtRender, FunctionAnalyser

from ._lib.extra import increment, sum

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


@subworkflow()
def to_int_bad(num: int, should_double: bool) -> int | float:
    """Cast to an int."""
    return increment(num=num) if should_double else sum(left=num, right=num)

@subworkflow()
def to_int(num: int, should_double: AtRender[bool]) -> int | float:
    """Cast to an int."""
    return increment(num=num) if should_double else sum(left=num, right=num)

def test_can_analyze_annotations():
    my_obj = MyClass()

    analyser = FunctionAnalyser(my_obj.method)
    assert analyser.argument_has("arg1", AtRender) is False
    assert analyser.argument_has("arg3", AtRender) is False
    assert analyser.argument_has("ARG2", AtRender) is False
    assert analyser.argument_has("arg2", AtRender) is True
    assert analyser.argument_has("arg4", AtRender) is False # Not a global/argument
    assert analyser.argument_has("ARG1", AtRender) is True

    analyser = FunctionAnalyser(fn)
    assert analyser.argument_has("arg5", AtRender) is False
    assert analyser.argument_has("arg7", AtRender) is False
    assert analyser.argument_has("ARG2", AtRender) is False
    assert analyser.argument_has("arg2", AtRender) is True
    assert analyser.argument_has("arg8", AtRender) is False # Not a global/argument
    assert analyser.argument_has("ARG1", AtRender) is True

def test_at_construct() -> None:
    with pytest.raises(TaskException) as _:
        result = to_int_bad(num=increment(num=3), should_double=True)
        workflow = construct(result, simplify_ids=True)

    result = to_int(num=increment(num=3), should_double=True)
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow, allow_complex_types=True)
    rendered = subworkflows["__root__"]
    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-1-num:
            default: 3
            label: increment-1-num
            type: int
        outputs:
          out:
            label: out
            outputSource: to_int-1/out
            type:
            - int
            - double
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
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow, allow_complex_types=True)
    rendered = subworkflows["__root__"]
    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          increment-1-num:
            default: 3
            label: increment-1-num
            type: int
        outputs:
          out:
            label: out
            outputSource: to_int-1/out
            type:
            - int
            - double
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
