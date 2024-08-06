from dewret.renderers.snakemake import to_snakemake_type

def test_to_snakemake_type_str():
    result = to_snakemake_type("str|value")
    assert result == '"value"'

def test_to_snakemake_type_bool():
    result = to_snakemake_type("bool|True")
    assert result == "True"

def test_to_snakemake_type_dict():
    result = to_snakemake_type("dict|{}")
    assert result == "{}"

def test_to_snakemake_type_list():
    result = to_snakemake_type("list|[]")
    assert result == "[]"

def test_to_snakemake_type_float():
    result = to_snakemake_type("float|3.14")
    assert result == "3.14"

def test_to_snakemake_type_int():
    result = to_snakemake_type("int|42")
    assert result == "42"

def test_to_snakemake_type_invalid():
    try:
        to_snakemake_type("complex|value")
    except TypeError as e:
        assert str(e) == "Cannot render complex type (complex|value)"
