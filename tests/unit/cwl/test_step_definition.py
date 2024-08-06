from dewret.renderers.cwl import StepDefinition, Reference, ReferenceDefinition
from dewret.workflow import Raw


# Mock Task
class MockTask:
    def __init__(self, name="mock_task"):
        self.name = name


# Mock a Reference
class MockReference(Reference):
    def __init__(self, name):
        self._name = name

    @property
    def name(self) -> str:
        return self._name


# Mock a baseStep
class MockBaseStep:
    def __init__(self, name="mock_step", task=None, return_type=str, arguments=None):
        self.name = name
        self.task = task or MockTask()
        self.return_type = return_type
        self.arguments = arguments or {}


def test_step_definition_from_step_basic():
    """Test creating a StepDefinition from a simple BaseStep."""
    step = MockBaseStep()

    step_def = StepDefinition.from_step(step)

    assert step_def.name == "mock_step"
    assert step_def.run == "mock_task"
    assert step_def.out == ["out"]
    assert step_def.in_ == {}


def test_step_definition_from_step_with_inputs():
    """Test creating a StepDefinition with inputs."""
    step = MockBaseStep(
        arguments={"in1": MockReference("input_ref"), "in2": Raw("raw_value")}
    )

    step_def = StepDefinition.from_step(step)

    assert step_def.name == "mock_step"
    assert step_def.run == "mock_task"
    assert step_def.out == ["out"]
    assert len(step_def.in_) == 2
    assert isinstance(step_def.in_["in1"], ReferenceDefinition)
    assert isinstance(step_def.in_["in2"], Raw)


def test_step_definition_render():
    """Test rendering a StepDefinition to a dict structure."""
    # Create a mock StepDefinition
    step_def = StepDefinition(
        name="mock_step",
        run="mock_task",
        out=["out"],
        in_={"in1": ReferenceDefinition("input_ref"), "in2": Raw("raw_value")},
    )

    # Render the StepDefinition
    rendered = step_def.render()

    # Assert the rendered output is correct
    assert rendered == {
        "run": "mock_task",
        "in": {"in1": {"source": "input_ref"}, "in2": {"default": "raw_value"}},
        "out": ["out"],
    }
