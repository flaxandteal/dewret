from dewret.renderers.snakemake import get_method_args, get_method_rel_path


def test_get_method_args():
    def sample_func(a: int, b: str) -> None:
        pass

    args = get_method_args(sample_func)
    assert list(args.parameters.keys()) == ["a", "b"]
    assert args.parameters["a"].annotation == int
    assert args.parameters["b"].annotation == str


def test_get_method_rel_path():
    def sample_func():
        pass

    relative_path = get_method_rel_path(sample_func)
    assert isinstance(relative_path, str)
    assert relative_path.endswith("test_helper_methods")
