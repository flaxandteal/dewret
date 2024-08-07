import yaml
from dewret.tasks import construct, task, factory
from dewret.renderers.cwl import render

from ._lib.extra import pi

@task()
def reverse_list(to_sort: list[int]) -> list[int]:
    return to_sort[::-1]

def test_can_supply_nested_raw():
    result = reverse_list(to_sort=[1, 3, pi()])
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]
    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          reverse_list-1-to_sort:
            default: [1, 3, 5]
            label: reverse_list-1-to_sort
            type:
              type:
                items: int
                label: reverse_list-1-to_sort
                type: array
        outputs:
          out:
            items: int
            label: out
            outputSource: reverse_list-1/out
            type: array
        steps:
          reverse_list-1:
            in:
              to_sort:
                source: reverse_list-1-to_sort
            out:
            - out
            run: reverse_list
    """)
