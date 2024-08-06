from dewret.renderers.cwl import set_configuration, configuration


def test_default_configuration():
    """Test the default configuration."""
    set_configuration({})

    assert configuration("allow_complex_types") is False
    assert configuration("factories_as_params") is False


def test_custom_configuration():
    """Test setting and retrieving custom configuration."""
    custom_config = {"allow_complex_types": True, "factories_as_params": True}
    set_configuration(custom_config)

    assert configuration("allow_complex_types") is True
    assert configuration("factories_as_params") is True


def test_unknown_configuration_key():
    """Test retrieving an unknown configuration key."""
    set_configuration({"allow_complex_types": True})

    assert configuration("allow_complex_types") is True

    try:
        configuration("unknown_key")
    except KeyError as e:
        assert str(e) == "'Unknown configuration settings.'"
    else:
        raise AssertionError("Expected KeyError not raised.")
