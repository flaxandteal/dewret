import yaml
import pytest
from dewret.tasks import construct, task, factory, subworkflow
from dewret.renderers.cwl import render
from dewret.utils import hasher
from dewret.tasks import set_configuration
from ._lib.extra import increment, double, mod10, sum, triple_and_one

@pytest.fixture
def configuration():
    with set_configuration() as configuration:
        yield configuration.get()

@subworkflow()
def floor(num: int | float, expected: bool) -> int:
    """Converts int/float to int."""
    from dewret.tasks import get_configuration
    if get_configuration("flatten_all_nested") != expected:
        raise AssertionError(f"Not expected configuration: {get_configuration('flatten_all_nested')} != {expected}")
    return int(num)

def test_cwl_with_parameter(configuration) -> None:
    result = increment(num=floor(num=3.1, expected=True))
    workflow = construct(result)
    rendered = render(workflow)["__root__"]
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
