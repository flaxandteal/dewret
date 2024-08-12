import yaml
import pytest
import math
from dewret.workflow import param
from dewret.tasks import construct, task, factory
from dewret.renderers.cwl import render

from ._lib.extra import reverse_list, max_list

def test_can_supply_nested_raw():
    pi = param("pi", math.pi)
    result = reverse_list(to_sort=[1., 3., pi])
    workflow = construct(max_list(lst=result + result), simplify_ids=True)
    #assert workflow.find_parameters() == {
    #    pi
    #}

    # NB: This is not currently usefully renderable in CWL.
    # However, the structures are important for future CWL rendering.

    rendered = render(workflow)["__root__"]
    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          pi:
            default: 3.141592653589793
            label: pi
            type: double
        outputs:
          out:
            label: out
            outputSource: max_list-1/out
            type:
            - int
            - double
        steps:
          max_list-1:
            in:
              lst:
                expression: $(2*reverse_list-1)
            out:
            - out
            run: max_list
          reverse_list-1:
            in:
              to_sort:
                expression: $((1.0, 3.0, pi))
            out:
            - out
            run: reverse_list
    """)
