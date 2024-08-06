from dewret.renderers.snakemake import InputDefinition
from dewret.workflow import Reference, Raw, BaseStep


class MockReference(Reference):
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name


class MockBaseStep(BaseStep):
    def __init__(self, arguments):
        self.arguments = arguments


class MockRaw(Raw):
    def __init__(self, value):
        self.value = value


def test_input_definition_from_step():
    ref1 = MockReference("step1")
    ref2 = MockReference("step2")
    step = MockBaseStep(
        arguments={"input1": ref1, "param1": MockRaw("str|default"), "input2": ref2}
    )

    input_def = InputDefinition.from_step(step)

    expected_inputs = [
        "input1=rules.step1.output_file",
        "input2=rules.step2.output_file",
    ]
    expected_params = [
        "input1=rules.step1.output_file,",
        'param1="default",',
        "input2=rules.step2.output_file",
    ]

    assert input_def.inputs == expected_inputs
    assert input_def.params == expected_params


def test_input_definition_render():
    input_def = InputDefinition(
        inputs=["input1=rules.step1.output_file", "input2=rules.step2.output_file"],
        params=[
            "input1=rules.step1.output_file,",
            "param1=str|default",
            "input2=rules.step2.output_file",
        ],
    )
    rendered = input_def.render()

    expected_render = {
        "inputs": ["input1=rules.step1.output_file", "input2=rules.step2.output_file"],
        "params": [
            "input1=rules.step1.output_file,",
            "param1=str|default",
            "input2=rules.step2.output_file",
        ],
    }

    assert rendered == expected_render


def test_input_definition_multiple():
    step2 = MockBaseStep(
        {
            "param1": MockRaw("value1"),
            "param2": MockReference("ref2"),
            "param3": MockRaw("value2"),
        }
    )

    input_def2 = InputDefinition.from_step(step2)
    assert input_def2.inputs == ["param2=rules.ref2.output_file"]
    assert input_def2.params == [
        'param1="value1",',
        "param2=rules.ref2.output_file,",
        'param3="value2"',
    ]


def test_input_definition_no_references():
    step3 = MockBaseStep(
        {
            "param1": MockRaw("value1"),
            "param2": MockRaw("value2"),
        }
    )

    input_def3 = InputDefinition.from_step(step3)
    assert input_def3.inputs == []
    assert input_def3.params == ['param1="value1",', 'param2="value2"']


def test_input_definition_empty():
    step4 = MockBaseStep({})

    input_def4 = InputDefinition.from_step(step4)
    assert input_def4.inputs == []
    assert input_def4.params == []
