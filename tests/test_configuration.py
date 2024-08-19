import yaml
import pytest
from dewret.tasks import construct, task, factory, workflow, TaskException
from dewret.renderers.cwl import render
from dewret.utils import hasher
from dewret.tasks import set_configuration
from dewret.annotations import AtRender
from ._lib.extra import increment, double, mod10, sum, triple_and_one

@pytest.fixture
def configuration():
    with set_configuration() as configuration:
        yield configuration.get()

@workflow()
def floor(num: int, expected: AtRender[bool]) -> int:
    """Converts int/float to int."""
    from dewret.tasks import get_configuration
    if get_configuration("flatten_all_nested") != expected:
        raise AssertionError(f"Not expected configuration: {get_configuration('flatten_all_nested')} != {expected}")
    return increment(num=num)

def test_cwl_with_parameter(configuration) -> None:
    with set_configuration(flatten_all_nested=True):
        result = increment(num=floor(num=3, expected=True))
        workflow = construct(result, simplify_ids=True)

    with pytest.raises(TaskException) as exc, set_configuration(flatten_all_nested=False):
        result = increment(num=floor(num=3, expected=True))
        workflow = construct(result, simplify_ids=True)
    assert "AssertionError" in str(exc.getrepr())

    with set_configuration(flatten_all_nested=True):
        result = increment(num=floor(num=3, expected=True))
        workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]
    num_param = list(workflow.find_parameters())[0]

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs:
          num:
            label: num
            type: int
            default: 3
        outputs:
          out:
            label: out
            outputSource: increment-1/out
            type: int
        steps:
          increment-2:
            run: increment
            in:
                num:
                    source: num
            out: [out]
          increment-1:
            run: increment
            in:
                num:
                    source: increment-2/out
            out: [out]
    """)
