from dewret.renderers.cwl import ReferenceDefinition

class MockReference:
    """Mock class to simulate a `Reference` object."""
    def __init__(self, name: str):
        self.name = name

def test_reference_definition_from_reference():
    """Test the `from_reference` class method."""
    ref = MockReference(name="test_step")
    
    # Create a ReferenceDefinition from a Reference
    ref_def = ReferenceDefinition.from_reference(ref)
    
    # Assert the source is correctly set
    assert ref_def.source == "test_step"

def test_reference_definition_render():
    """Test the `render` method."""
    # Create a ReferenceDefinition instance
    ref_def = ReferenceDefinition(source="test_step")
    
    # Render the instance to a dict
    rendered = ref_def.render()
    
    # Expected rendered dict
    expected = {"source": "test_step"}
    
    # Assert the rendered dict is as expected
    assert rendered == expected

def test_reference_definition_render_empty_source():
    """Test the `render` method with an empty source."""
    # Create a ReferenceDefinition instance with an empty source
    ref_def = ReferenceDefinition(source="")
    
    # Render the instance to a dict
    rendered = ref_def.render()
    
    # Expected rendered dict
    expected = {"source": ""}
    
    # Assert the rendered dict is as expected
    assert rendered == expected