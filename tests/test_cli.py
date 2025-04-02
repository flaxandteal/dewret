"""Test CLI render function."""

import yaml
import pytest
from pathlib import Path
from dewret.cli import render


def test_cli_call() -> None:
    """Test CLI render function."""
    # Setup
    test_dir = Path(__file__).parent
    repo_root = test_dir.parent

    example_file = repo_root / "example" / "workflow_qs.py"
    output_file = (test_dir / "tmp_file").resolve()

    try:
        # not sure why it is raising an exception
        with pytest.raises(SystemExit) as exc_info:
            render(
                [
                    "--pretty",
                    "--backend",
                    "DASK",
                    "--output",
                    str(output_file),
                    example_file.as_posix(),
                    "increment",
                    "num:3",
                ]
            )

        # Check that it was a successful exit (code 0)
        assert exc_info.value.code == 0

        # Now check the output file
        with open(output_file, "r") as file:
            res = yaml.safe_load(file)

        assert list(res.keys()) == ["class", "cwlVersion", "inputs", "outputs", "steps"]
    finally:
        # Delete the file in the finally block to ensure it's always cleaned up
        if output_file.exists():
            output_file.unlink()
