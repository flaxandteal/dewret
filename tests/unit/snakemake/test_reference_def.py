from dewret.renderers.snakemake import ReferenceDefinition


class Reference:
    """Mock class representing a Reference object."""

    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name


def test_from_reference():
    ref = Reference("test_reference")
    print(ref.name)
    ref_def = ReferenceDefinition.from_reference(ref)
    assert (
        ref_def.source == "test_reference"
    ), f"Expected 'test_reference', got {ref_def.source}"


def test_reference_render():
    ref = Reference("test_reference")
    ref_def = ReferenceDefinition.from_reference(ref)
    output = ref_def.render()
    assert output == "test_reference", f"Expected 'test_reference', got {output}"
