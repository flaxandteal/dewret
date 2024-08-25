"""Check field management works."""

from __future__ import annotations
import yaml
from dataclasses import dataclass

from typing import Unpack, TypedDict

from dewret.tasks import task, construct, workflow
from dewret.core import set_configuration
from dewret.workflow import param, StepReference
from dewret.renderers.cwl import render
from dewret.annotations import Fixed

from ._lib.extra import mod10, sum, pi

@dataclass
class Sides:
    """TODO: Docstring."""
    left: int
    right: int

SIDES: Sides = Sides(3, 6)

@workflow()
def sum_sides() -> float:
    """TODO: Docstring."""
    return sum(left=SIDES.left, right=SIDES.right)

def test_fields_of_parameters_usable() -> None:
    """TODO: Docstring."""
    result = sum_sides()
    wkflw = construct(result, simplify_ids=True)
    rendered = render(wkflw, allow_complex_types=True)["sum_sides-1"]

    assert rendered == yaml.safe_load("""
      class: Workflow
      cwlVersion: 1.2
      inputs:
        SIDES:
          label: SIDES
          default:
            left: 3
            right: 6
          type: record
          fields:
            left:
              default: 3
              label: left
              type: int
            right:
              default: 6
              label: right
              type: int
          label: SIDES
      outputs:
        out:
          label: out
          outputSource: sum-1-1/out
          type:
          - int
          - float
      steps:
        sum-1-1:
          in:
            left:
              source: SIDES/left
            right:
              source: SIDES/right
          out:
          - out
          run: sum
    """)

@dataclass
class MyDataclass:
    """TODO: Docstring."""
    left: int
    right: "MyDataclass"

def test_can_get_field_reference_from_parameter() -> None:
    """TODO: Docstring."""
    my_param = param("my_param", typ=MyDataclass)
    result = sum(left=my_param.left, right=sum(left=my_param.right.left, right=my_param.left))
    wkflw = construct(result, simplify_ids=True)
    params = {(str(p), p.__type__) for p in wkflw.find_parameters()}

    assert params == {("my_param", MyDataclass)}
    rendered = render(wkflw, allow_complex_types=True)["__root__"]
    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          my_param:
            label: my_param
            type: MyDataclass
        outputs:
          out:
            label: out
            outputSource: sum-1/out
            type:
            - int
            - float
        steps:
          sum-1:
            in:
              left:
                source: my_param/left
              right:
                source: sum-2/out
            out:
            - out
            run: sum
          sum-2:
            in:
              left:
                source: my_param/right/left
              right:
                source: my_param/left
            out:
            - out
            run: sum
    """)

def test_can_get_field_reference_iff_parent_type_has_field() -> None:
    """TODO: Docstring."""
    @dataclass
    class MyDataclass:
        left: int
    my_param = param("my_param", typ=MyDataclass)
    result = sum(left=my_param.left, right=my_param.left)
    wkflw = construct(result, simplify_ids=True)
    param_reference = list(wkflw.find_parameters())[0]

    assert str(param_reference.left) == "my_param/left"
    assert param_reference.left.__type__ == int

def test_can_get_go_upwards_from_a_field_reference() -> None:
    """TODO: Docstring."""
    @dataclass
    class MyDataclass:
        left: int
        right: "MyDataclass"
    my_param = param("my_param", typ=MyDataclass)
    result = sum(left=my_param.left, right=my_param.left)
    construct(result, simplify_ids=True)

    back = my_param.right.left.__field_up__() # type: ignore
    assert str(back) == "my_param/right"
    assert back.__type__ == MyDataclass

def test_can_get_field_references_from_dataclass() -> None:
    """TODO: Docstring."""
    @dataclass
    class MyDataclass:
        left: int
        right: float

    @workflow()
    def test_dataclass(my_dataclass: MyDataclass) -> MyDataclass:
        result: MyDataclass = MyDataclass(left=mod10(num=my_dataclass.left), right=pi())
        return result

    @workflow()
    def get_left(my_dataclass: MyDataclass) -> int:
        return my_dataclass.left

    result = get_left(my_dataclass=test_dataclass(my_dataclass=MyDataclass(left=3, right=4.)))
    wkflw = construct(result, simplify_ids=True)

    assert isinstance(wkflw.result, StepReference)
    assert str(wkflw.result) == "get_left-1"
    assert wkflw.result.__type__ == int

class MyDict(TypedDict):
    """TODO: Docstring."""
    left: int
    right: float

def test_can_get_field_references_from_typed_dict() -> None:
    """TODO: Docstring."""
    @workflow()
    def test_dict(**my_dict: Unpack[MyDict]) -> MyDict:
        result: MyDict = {"left": mod10(num=my_dict["left"]), "right": pi()}
        return result

    result = test_dict(left=3, right=4.)
    wkflw = construct(result, simplify_ids=True)

    assert isinstance(wkflw.result, StepReference)
    assert str(wkflw.result["left"]) == "test_dict-1/left"
    assert wkflw.result["left"].__type__ == int

@dataclass
class MyListWrapper:
    """TODO: Docstring."""
    my_list: list[int]

def test_can_iterate() -> None:
    """TODO: Docstring."""
    @task()
    def test_task(alpha: int, beta: float, charlie: bool) -> int:
        return int(alpha + beta)

    @task()
    def test_list() -> list[int | float]:
        return [1, 2.]

    @workflow()
    def test_iterated() -> int:
        # We ignore the type as mypy cannot confirm that the length and types match the args.
        return test_task(*test_list()) # type: ignore

    with set_configuration(allow_positional_args=True, flatten_all_nested=True):
        result = test_iterated()
        wkflw = construct(result, simplify_ids=True)

    rendered = render(wkflw, allow_complex_types=True)["__root__"]

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs: {}
        outputs:
          out:
            label: out
            outputSource: test_task-1/out
            type: int
        steps:
          test_list-1:
            in: {}
            out:
            - out
            run: test_list
          test_task-1:
            in:
              alpha:
                source: test_list-1[0]
              beta:
                source: test_list-1[1]
              charlie:
                source: test_list-1[2]
            out:
            - out
            run: test_task
    """)

    assert isinstance(wkflw.result, StepReference)
    assert wkflw.result._.step.positional_args == {"alpha": True, "beta": True, "charlie": True}

    @task()
    def test_list_2() -> MyListWrapper:
        return MyListWrapper(my_list=[1, 2])

    @workflow()
    def test_iterated_2(my_wrapper: MyListWrapper) -> int:
        # mypy cannot confirm argument types match.
        return test_task(*my_wrapper.my_list) # type: ignore

    with set_configuration(allow_positional_args=True, flatten_all_nested=True):
        result = test_iterated_2(my_wrapper=test_list_2())
        wkflw = construct(result, simplify_ids=True)

    @task()
    def test_list_3() -> Fixed[list[tuple[int, int]]]:
        return [(0, 1), (2, 3)]

    @workflow()
    def test_iterated_3(param: Fixed[list[tuple[int, int]]]) -> int:
        # mypy cannot confirm argument types match.
        retval = mod10(*test_list_3()[0]) # type: ignore
        for pair in param:
            a, b = pair
            retval += a + b
        return mod10(retval)

    with set_configuration(allow_positional_args=True, flatten_all_nested=True):
        result = test_iterated_3(param=[(0, 1), (2, 3)])
        wkflw = construct(result, simplify_ids=True)

    rendered = render(wkflw, allow_complex_types=True)["__root__"]

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          param:
            default:
            - - 0
              - 1
            - - 2
              - 3
            items: array
            label: param
            type: array
        outputs:
          out:
            label: out
            outputSource: mod10-1/out
            type: int
        steps:
          mod10-1:
            in:
              num:
                valueFrom: $(inputs.param[0][0] + inputs.param[0][1] + inputs.param[1][0] + inputs.param[1][1] + self)
                source: mod10-2/out
            out:
            - out
            run: mod10
          mod10-2:
            in:
              num:
                source: test_list_3-1[0]
            out:
            - out
            run: mod10
          test_list_3-1:
            in: {}
            out:
            - out
            run: test_list_3
    """)

def test_can_use_plain_dict_fields() -> None:
    """TODO: Docstring."""
    @workflow()
    def test_dict(left: int, right: float) -> dict[str, float | int]:
        result: dict[str, float | int] = {"left": mod10(num=left), "right": pi()}
        return result

    with set_configuration(allow_plain_dict_fields=True):
        result = test_dict(left=3, right=4.)
        wkflw = construct(result, simplify_ids=True)
        assert isinstance(wkflw.result, StepReference)
        assert str(wkflw.result["left"]) == "test_dict-1/left"
        assert wkflw.result["left"].__type__ == int | float

@dataclass
class IndexTest:
    """TODO: Docstring."""
    left: Fixed[list[int]]

def test_can_configure_field_separator() -> None:
    """TODO: Docstring."""
    @task()
    def test_sep() -> IndexTest:
        return IndexTest(left=[3])

    with set_configuration(field_index_types="int"):
        result = test_sep().left[0]
        wkflw = construct(result, simplify_ids=True)
        render(wkflw, allow_complex_types=True)["__root__"]
        assert str(wkflw.result) == "test_sep-1/left[0]"

    with set_configuration(field_index_types="int,str"):
        result = test_sep().left[0]
        wkflw = construct(result, simplify_ids=True)
        render(wkflw, allow_complex_types=True)["__root__"]
        assert str(wkflw.result) == "test_sep-1[left][0]"

    with set_configuration(field_index_types=""):
        result = test_sep().left[0]
        wkflw = construct(result, simplify_ids=True)
        render(wkflw, allow_complex_types=True)["__root__"]
        assert str(wkflw.result) == "test_sep-1/left/0"
