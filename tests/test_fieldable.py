import yaml
from dataclasses import dataclass

import pytest
from typing import Unpack, TypedDict

from dewret.tasks import task, construct, subworkflow
from dewret.workflow import param
from dewret.renderers.cwl import render

from ._lib.extra import double, mod10, sum, pi

@dataclass
class Sides:
    left: int
    right: int

SIDES: Sides = Sides(3, 6)

@subworkflow()
def sum_sides():
    return sum(left=SIDES.left, right=SIDES.right)

@pytest.mark.skip(reason="Need expression support")
def test_fields_of_parameters_usable() -> None:
    result = sum_sides()
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow, allow_complex_types=True)["sum_sides-1"]

    assert rendered == yaml.safe_load("""
      class: Workflow
      cwlVersion: 1.2
      inputs:
        SIDES:
          label: SIDES
          type: record
          items:
            left: int
            right: int
      outputs:
        out:
          label: out
          outputSource: sum-1-1/out
          type:
          - int
          - double
      steps:
        sum-1-1:
          in:
            left:
              source: SIDES
              valueFrom: $(self.left)
            right:
              source: SIDES
              valueFrom: $(self.right)
          out:
          - out
          run: sum
    """)

def test_can_get_field_reference_iff_parent_type_has_field():
    @dataclass
    class MyDataclass:
        left: int
    my_param = param("my_param", typ=MyDataclass)
    result = sum(left=my_param, right=my_param)
    workflow = construct(result, simplify_ids=True)
    param_reference = list(workflow.find_parameters())[0]

    assert str(param_reference.left) == "my_param/left"
    assert param_reference.left.__type__ == int

def test_can_get_field_references_from_dataclass():
    @dataclass
    class MyDataclass:
        left: int
        right: float

    @subworkflow()
    def test_dataclass(my_dataclass: MyDataclass) -> MyDataclass:
        result: MyDataclass = MyDataclass(left=mod10(num=my_dataclass.left), right=pi())
        return result

    @subworkflow()
    def get_left(my_dataclass: MyDataclass) -> int:
        return my_dataclass.left

    result = get_left(my_dataclass=test_dataclass(my_dataclass=MyDataclass(left=3, right=4.)))
    workflow = construct(result, simplify_ids=True)

    assert str(workflow.result) == "get_left-1"
    assert workflow.result.__type__ == int

def test_can_get_field_references_from_typed_dict():
    class MyDict(TypedDict):
        left: int
        right: float

    @subworkflow()
    def test_dict(**my_dict: Unpack[MyDict]) -> MyDict:
        result: MyDict = {"left": mod10(num=my_dict["left"]), "right": pi()}
        return result

    result = test_dict(left=3, right=4.)
    workflow = construct(result, simplify_ids=True)

    assert str(workflow.result["left"]) == "test_dict-1/left"
    assert workflow.result["left"].__type__ == int
