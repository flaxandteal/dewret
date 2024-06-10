"""Basic snakemake workflow.

Useful as an example of a workflow with multiple tasks.

```sh
$ python -m dewret workflow_complex.py --pretty run
```
"""

# from dewret.workflow import Lazy
import os
import inspect
from dewret.tasks import task, construct
from renderer import render

STARTING_NUMBER: int = 23


def print_obj(obj):
    print(f"Type of obj:\n{type(obj)}\n")
    print(
        "--------------------------------------------------------------------------------------"
    )
    print(f"Members of obj:\n{dir(obj)}\n")
    print(
        "--------------------------------------------------------------------------------------"
    )
    print(f"obj:\n{obj}\n")
    print(
        "--------------------------------------------------------------------------------------"
    )

# @task()
def download_data(
    data: str,
    output_file: str,
) -> str:
    print("successfully enters this shit")

    with open(output_file, "w") as f:
        f.write(data)

    return output_file


# @task()
def process_data(data_file: str, multiple_arg: str, output_file: str) -> str:
    # Simulate downloaded data content
    new_data = "test"

    with open(data_file, "r") as f:
        data = f.read(data)
        new_data = data + new_data

    with open(output_file, "w") as f:
        f.write(new_data)

    return output_file


# @task()
def generate_report(processed_data: str, multiple_arg: str, output_file: str) -> str:
    # Read the processed data
    with open(processed_data, "r") as datafile:
        data = datafile.read()

    # Write the report
    with open(processed_data, "w") as f:
        f.write(data)

    return output_file


if __name__ == "__main__":
    data = download_data(
        data="4, 5, 7, 8, 0",
        output_file="generated_smk/data/test_data.txt",
    )
    processed_data = process_data(
        data_file=data,
        multiple_arg="test1",
        output_file="generated_smk/data/processed_data.txt",
    )
    result = generate_report(
        processed_data=processed_data, multiple_arg="test2", output_file="results/report.txt"
    )

    # workflow = construct(generate_report(data_file=process_data(data_file=download_data(data=str("1, 2, 3, 4, 5, 6, 7"), output_file="generated_smk/data/test_data.txt"))))
    workflow = construct(result)
    smk_output = render(workflow)

    with open("Snakefile", "w") as file:
        file.write(smk_output)
