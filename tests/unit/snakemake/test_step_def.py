from dewret.renderers.snakemake import StepDefinition, InputDefinition, RunDefinition, OutputDefinition, BaseStep
from unittest.mock import MagicMock

def mock_input_def_from_step(step):
    indef = InputDefinition(inputs=["inp1", "inp2"], params=["in1", "in2"])
    return indef

def mock_output_def_from_step(step):
    outdef = OutputDefinition(output_file="outf")
    return outdef

def mock_run_def_from_task(task):
    rundef = RunDefinition(
        method_name = "test_method",
        rel_import = "module.path",
        args = ["arg1", "arg2"]
    )
    return rundef

def test_from_step():
    step = MagicMock()
    step.name = "step_name"

    in_def = InputDefinition
    out_def = OutputDefinition
    run_def = RunDefinition

    in_def.from_step = mock_input_def_from_step
    out_def.from_step = mock_output_def_from_step
    run_def.from_task = mock_run_def_from_task


    result = StepDefinition.from_step(step)
    
    assert result.name == "step_name"
    assert result.input == ['inp1', 'inp2']
    assert result.run == ['import module.path\n', 'module.path.test_method(arg1, arg2)\n']
    assert result.output == ['output_file=outf']
    assert result.params == ['in1', 'in2']
