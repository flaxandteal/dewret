import yaml
from dataclasses import dataclass
from dewret.tasks import task, construct, subworkflow
from dewret.workflow import param
from dewret.renderers.cwl import render

from ._lib.extra import double, mod10, sum

@dataclass
class Sides:
    left: int
    right: int

SIDES: Sides = Sides(3, 6)

@subworkflow()
def sum_sides():
    return sum(left=SIDES.left, right=SIDES.right)

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
