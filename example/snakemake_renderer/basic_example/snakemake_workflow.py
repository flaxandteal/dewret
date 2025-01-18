"""Basic snakemake workflow.

Useful as an example of a workflow with multiple tasks.

```sh
$ python snakemake_workflow.py
```
Since snakemake commonly communicates via the filesystem between workflow steps,
each our tasks and hence steps writes its output to a file.
This output can then be read by subsequent steps.

For the current implementation, output files must be called "output_file".

Running the example will generate a Snakefile with the following code:

rule download_data_cbb4d1260b8b17d6693986ebfd833d53:
    input:
    output:
        output_file="generated_smk/data/test_data.txt"
    params:
        data="4, 5, 7, 8, 0",
        output_file="generated_smk/data/test_data.txt"
    run:
        import snakemake_tasks


        snakemake_tasks.download_data(data=params.data, output_file=params.output_file)


rule generate_report_b1f9747ed9903dcd9571246547b1767b:
    input:
        processed_data=rules.process_data_75213b5c31fed773cd1f7ca1dfb6ab5a.output.output_file
    output:
        output_file="results/report.txt"
    params:
        processed_data=rules.process_data_75213b5c31fed773cd1f7ca1dfb6ab5a.output.output_file,
        multiple_arg="test2",
        output_file="results/report.txt"
    run:
        import snakemake_tasks


        snakemake_tasks.generate_report(processed_data=params.processed_data, multiple_arg=params.multiple_arg,
        output_file=params.output_file)


rule process_data_75213b5c31fed773cd1f7ca1dfb6ab5a:
    input:
        data_file=rules.download_data_cbb4d1260b8b17d6693986ebfd833d53.output.output_file
    output:
        output_file="generated_smk/data/processed_data.txt"
    params:
        data_file=rules.download_data_cbb4d1260b8b17d6693986ebfd833d53.output.output_file,
        multiple_arg="test1",
        output_file="generated_smk/data/processed_data.txt"
    run:
        import snakemake_tasks


        snakemake_tasks.process_data(data_file=params.data_file, multiple_arg=params.multiple_arg,
        output_file=params.output_file)

In order to make the snakemake file be executable you need to:
- manually add:
rule all:
    input:
        "results/report.txt" # To make sure all connected rules are executed. For more information https://snakemake.readthedocs.io/
- Change the order of the rules to match the order of execution.
- Make sure all methods in run blocks are on the same line.
- Remove @task annotation from the snakemake_workflow.py
"""

import yaml
from dewret.tasks import task, construct
from dewret.renderers.snakemake import render


@task()
def download_data(data: str, output_file: str) -> str:
    """Example task to be converted to snakemake workflow rule.

    This function writes the given data to the specified output file.

    Args:
        data (str): The data to be written to the output file.
        output_file (str): The path to the output file where the data will be written.

    Returns:
        str: The path to the output file.
    """
    with open(output_file, "w") as f:
        f.write(data)

    return output_file


@task()
def process_data(data_file: str, multiple_arg: str, output_file: str) -> str:
    """Example task to be converted to snakemake workflow rule.

    This function reads data from a file, processes it by appending additional
    strings, and writes the result to an output file.

    Args:
        data_file (str): The path to the input file containing the data to be processed.
        multiple_arg (str): An additional string to be appended during processing.
        output_file (str): The path to the output file where the processed data will be written.

    Returns:
        str: The path to the output file.
    """
    new_data = "test"

    with open(data_file, "r") as f:
        data = f.read()
        new_data = data + new_data + multiple_arg

    with open(output_file, "w") as f:
        f.write(new_data)

    return output_file


@task()
def generate_report(processed_data: str, multiple_arg: str, output_file: str) -> str:
    """Example task to be converted to snakemake workflow.

    This function generates a report by reading processed data from a file and
    appending an additional string, then writes the report to an output file.

    Args:
        processed_data (str): The path to the input file containing the processed data.
        multiple_arg (str): An additional string to be appended to the report.
        output_file (str): The path to the output file where the report will be written.

    Returns:
        str: The path to the output file.
    """
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
        trans_table = str.maketrans(
            {
                "-": "   ",
                "'": "",
                "[": "",
                "]": "",
            }
        )
        smk_text = yaml.dump(smk_output, indent=4).translate(trans_table)
        file.write(smk_text)
