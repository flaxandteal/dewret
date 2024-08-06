from dewret.renderers.snakemake import OutputDefinition
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



def test_output_definition_basic():
    step1 = MockBaseStep({
        'output_file': MockRaw('raw_output'),
    })

    output_def1 = OutputDefinition.from_step(step1)
    assert output_def1.output_file == '"raw_output"'
    assert output_def1.render() == ['output_file="raw_output"']

def test_output_definition_empty():
    step3 = MockBaseStep({
        'output_file': '',
    })

    output_def3 = OutputDefinition.from_step(step3)
    assert output_def3.output_file == ''
    assert output_def3.render() == ['output_file=']

def test_output_definition_invalid_type():
    step4 = MockBaseStep({
        'output_file': 123,
    })

    output_def4 = OutputDefinition.from_step(step4)
    assert output_def4.output_file == ''
    assert output_def4.render() == ['output_file=']