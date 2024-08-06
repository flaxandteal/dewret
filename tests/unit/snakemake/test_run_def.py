from dewret.renderers.snakemake import RunDefinition, get_method_args, get_method_rel_path
from dewret.tasks import Task
from dewret.workflow import Workflow
from unittest.mock import MagicMock, patch
import inspect

# Mock definitions to mimic actual behavior
class MockTask(Task):
    def __init__(self, name, target):
        self.name = name
        self.target = target

class MockWorkflow(Workflow):
    pass

def mock_get_method_args(func):
    """Mock implementation of get_method_args for testing."""
    p1 = inspect.Parameter(name='param1', kind=inspect.Parameter.KEYWORD_ONLY, annotation=str)
    p2 = inspect.Parameter(name='param2', kind=inspect.Parameter.KEYWORD_ONLY, annotation=int)
    return inspect.Signature(
        parameters=[p1,p2]
    )

def mock_get_method_rel_path(func):
    """Mock implementation of get_method_rel_path for testing."""
    return 'module.path'

# Unit tests
@patch('dewret.renderers.snakemake.get_method_args', side_effect=mock_get_method_args)
@patch('dewret.renderers.snakemake.get_method_rel_path', side_effect=mock_get_method_rel_path)
def test_run_definition_basic(mock_get_method_args, mock_get_method_rel_path):
    task = MockTask('method_name', 'mock_target')
    run_def = RunDefinition.from_task(task)
    assert run_def.method_name == 'method_name'
    assert run_def.rel_import == 'module.path'
    assert run_def.args == ['param1=params.param1', 'param2=params.param2']
    assert run_def.render() == [
        "import module.path\n",
        "module.path.method_name(param1=params.param1, param2=params.param2)\n",
    ]

@patch('dewret.renderers.snakemake.get_method_args', side_effect=mock_get_method_args)
@patch('dewret.renderers.snakemake.get_method_rel_path', side_effect=mock_get_method_rel_path)
def test_run_definition_empty_args(mock_get_method_args, mock_get_method_rel_path):
    task = MockTask('method_name', 'mock_target_empty_args')
    run_def = RunDefinition.from_task(task)
    assert run_def.method_name == 'method_name'
    assert run_def.rel_import == 'module.path'
    assert run_def.args == ['param1=params.param1', 'param2=params.param2']
    assert run_def.render() == [
        "import module.path\n",
        "module.path.method_name(param1=params.param1, param2=params.param2)\n",
    ]


# Once implementation for workflows is added you can add this test
# @patch('dewret.renderers.snakemake.get_method_args', side_effect=mock_get_method_args)
# @patch('dewret.renderers.snakemake.get_method_rel_path', side_effect=mock_get_method_rel_path)
# def test_run_definition_no_task(mock_get_method_args, mock_get_method_rel_path):
#     workflow = MockWorkflow()
#     run_def = RunDefinition.from_task(workflow)
#     assert run_def.method_name == ''
#     assert run_def.rel_import == ''
#     assert run_def.args == []
#     assert run_def.render() == [
#         "import \n",
#         ".method_name()\n",
#     ]
