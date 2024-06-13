"""Basic snakemake workflow.

Useful as an example of a workflow with multiple tasks.

```sh
$ python -m dewret workflow_complex.py --pretty run
```
"""

from dewret.tasks import task, construct
from dewret.renderers.snakemake import render

STARTING_NUMBER: int = 23


@task()
def download_data(data: str, output_file: str) -> str:
    """Example task to be converted to snakemake workflow."""
    with open(output_file, "w") as f:
        f.write(data)

    return output_file


@task()
def process_data(data_file: str, multiple_arg: str, output_file: str) -> str:
    """Example task to be converted to snakemake workflow."""
    new_data = "test"

    with open(data_file, "r") as f:
        data = f.read()
        new_data = data + new_data + multiple_arg

    with open(output_file, "w") as f:
        f.write(new_data)

    return output_file


@task()
def generate_report(processed_data: str, multiple_arg: str, output_file: str) -> str:
    """Example task to be converted to snakemake workflow."""
    with open(processed_data, "r") as datafile:
        data = datafile.read()

    with open(output_file, "w") as f:
        f.write(data + multiple_arg)

    return output_file


if __name__ == "__main__":
    data = download_data(
        data="4, 5, 7, 8, 0", output_file="generated_smk/data/test_data.txt"
    )
    processed_data = process_data(
        data_file=data,
        multiple_arg="test1",
        output_file="generated_smk/data/processed_data.txt",
    )
    result = generate_report(
        processed_data=processed_data,
        multiple_arg="test2",
        output_file="results/report.txt",
    )

    workflow = construct(result)
    smk_output = render(workflow)

    with open("Snakefile", "w") as file:
        file.write(smk_output)
