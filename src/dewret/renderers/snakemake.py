# Copyright 2014 Flax & Teal Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Snakemake Renderer.

Outputs a [Snakemake](https://snakemake.github.io/) representation of the
current workflow.

"""

import os
import yaml
import inspect
import typing

from attrs import define

from dewret.core import (
    Raw,
    BasicType,
    Reference,
)
from dewret.workflow import (
    Workflow,
    Task,
    Lazy,
    BaseStep,
)
from dewret.render import (
    base_render,
)

MainTypes = BasicType | list[str] | list["MainTypes"] | dict[str, "MainTypes"]


@define
class ReferenceDefinition:
    """Represents a Snakemake-renderable internal reference.

    Attributes:
        source (str): The source of the internal reference.

    Methods:
        from_reference(cls, ref: Reference) -> "ReferenceDefinition": Constructs a
            ReferenceDefinition object from a Reference object, extracting the source
            of the reference.
        render(self) -> str: Renders the internal reference definition as a string
            suitable for use in Snakemake workflows.
    """

    source: str

    @classmethod
    def from_reference(cls, ref: Reference[typing.Any]) -> "ReferenceDefinition":
        """Build from a `Reference`.

        Converts a `dewret.workflow.Reference` into a Snakemake-rendering object.

        Args:
            ref: reference to convert.
        """
        return cls(source=ref.name)

    def render(self) -> str:
        """Render the internal reference definition as a string.

        Returns:
            str: internal reference.
        """
        return self.source


# TODO: Refactor: better way to handle the types.
def to_snakemake_type(param: Raw) -> str:
    """Convert a raw type to a corresponding Snakemake-compatible Python type.

    This function maps raw types to their corresponding Python types as
    used in Snakemake. Snakemake primarily uses Python types for its parameters,
    and this function ensures that the provided type is appropriately converted.

    Args:
        param: The raw type to be converted, which can be of any type.

    Returns:
        A string representing the corresponding Python type for Snakemake.

    Raises:
        TypeError: If the parameter's type cannot be mapped to a known Python type.
    """
    typ = str(param)
    if typ.__contains__("str"):
        return f'"{typ.replace("str|", "")}"'
    elif typ.__contains__("bool"):
        return typ.replace("bool|", "")
    elif typ.__contains__("dict"):
        return typ.replace("dict|", "")
    elif typ.__contains__("list"):
        return typ.replace("list|", "")
    elif typ.__contains__("float"):
        return typ.replace("float|", "")
    elif typ.__contains__("int"):
        return typ.replace("int|", "")
    else:
        raise TypeError(f"Cannot render complex type ({typ})")


def get_method_args(func: Lazy) -> inspect.Signature:
    """Retrieve the argument names and types of a lazy-evaluatable function.

    Args:
        func: A function that adheres to the `Lazy` protocol.

    Returns:
        An `ItemsView` object containing the argument names and their corresponding
        `inspect.Parameter` objects.
    """
    args = inspect.signature(func)

    return args


def get_method_rel_path(func: Lazy) -> typing.Any:
    """Get the relative path of the module containing the given function.

    Args:
        func (Lazy): The function for which the relative path is to be determined.

    Returns:
        any: The relative path of the module containing the function.

    Note:
        This function relies on the `inspect` and `os` modules to compute its relative path.
    """
    # TODO: error handling
    source_file = inspect.getsourcefile(func)
    if source_file:
        relative_path = os.path.relpath(source_file, start=os.getcwd())
        module_name = os.path.splitext(relative_path)[0].replace(os.path.sep, ".")

    return module_name


@define
class InputDefinition:
    """Represents input and parameter definitions block for a Snakemake-renderable workflow step.

    Attributes:
        inputs (List[str]): A list of input definitions.
        params (List[str]): A list of parameter definitions.

    Methods:
        from_step(cls, step: Step) -> "InputDefinition": Constructs an InputDefinition
            object from a Step object, extracting inputs and parameters and converting
            them to Snakemake-compatible format.
        render(self) -> dict[str, str]: Renders the input and parameter definitions
            as a dictionary for use in Snakemake Input and Params blocks.
    """

    inputs: list[str]
    params: list[str]

    @classmethod
    def from_step(cls, step: BaseStep) -> "InputDefinition":
        """Constructs an InputDefinition object from a Step.

        Args:
            step (Step): The Step object from which input and parameter block definitions are
                extracted.

        Returns:
            InputDefinition: An InputDefinition object.
        """
        params = []
        inputs = []
        for key, param in step.arguments.items():
            if isinstance(param, Reference):
                ref = (
                    ReferenceDefinition.from_reference(param)
                    .render()
                    .replace("-", "_")
                    .replace("/out", ".output")
                )
                input = f"{key}=rules.{ref}.output_file"
                inputs.append(input)
                params.append(input + ",")
            elif isinstance(param, Raw):
                customized = f"{key}={to_snakemake_type(param)},"
                params.append(customized)

        if params:
            params[len(params) - 1] = params[len(params) - 1].replace(",", "")

        return cls(inputs=inputs, params=params)

    def render(self) -> dict[str, list[str]]:
        """Renders the input and parameter definitions as a dictionary.

        Returns:
            dict[str, list[MainTypes]]: A dictionary containing the input and parameter definitions,
                for use in Snakemake Input and Params blocks.
        """
        return {"inputs": self.inputs, "params": self.params}


@define
class OutputDefinition:
    """Represents the output definition block for a Snakemake-renderable workflow step.

    Attributes:
        output_file (str): The output file definition.

    Methods:
        from_step(cls, step: Step) -> "OutputDefinition": Constructs an OutputDefinition
            object from a Step object, extracting and converting the output file definition
            to Snakemake-compatible format.

        render(self) -> list[str]: Renders the output definition as a list
            suitable for use in Snakemake Output block.
    """

    output_file: str

    @classmethod
    def from_step(cls, step: BaseStep) -> "OutputDefinition":
        """Constructs an OutputDefinition object from a Step.

        Args:
            step (Step): The Step object from which the output file definition is extracted.

        Returns:
            OutputDefinition: An OutputDefinition object, for use in Snakemake Output block.
        """
        # TODO: Error handling
        # TODO: Better way to handling input/output files
        output_file = step.arguments.get("output_file", Raw("OUTPUT_FILE"))
        if isinstance(output_file, Raw):
            args = to_snakemake_type(output_file)

        return cls(output_file=args)

    def render(self) -> list[str]:
        """Renders the output definition as a list.

        Returns:
            list[str]: A list containing the output file definition, for use in a Snakemake Output block.
        """
        # The comma after the last element is mandatory for the structure of rule onces it's used in yaml.dump
        return [
            f"output_file={self.output_file}",
        ]


@define
class RunDefinition:
    """Represents a Snakemake-renderable run block for a dewret workflow step.

    Attributes:
        method_name (str): The name of the method to be executed in the snakefile run block.
        rel_import (str): The relative import path of the method.
        args (List[str]): The arguments to be passed to the method.

    Methods:
        from_task(cls, task: Task) -> "RunDefinition": Constructs a RunDefinition
            object from a Task object, extracting method information and arguments
            from the task and converting them to Snakemake-compatible format.

        render(self) -> list[str]: A list containing the import statement and the method
            call statement, for use in Snakemake run block.
    """

    method_name: str
    rel_import: str
    args: list[str]

    @classmethod
    def from_task(cls, task: Task | Workflow) -> "RunDefinition":
        """Constructs a RunDefinition object from a Task.

        Args:
            task (Task): The Task object from which method information and arguments
                are extracted.

        Returns:
            RunDefinition: A RunDefinition object containing the converted method
                information and arguments.
        """
        if isinstance(task, Task):
            relative_path = get_method_rel_path(task.target)
            rel_import = f"{relative_path}"
            args = get_method_args(task.target)
        else:
            # TODO: Add implementation for multiple workflows
            pass

        signature = [
            f"{param_name}=params.{param_name}" for param_name in args.parameters.keys()
        ]

        return cls(method_name=task.name, rel_import=rel_import, args=signature)

    def render(self) -> list[str]:
        """Renders the run block as a list of strings.

        Returns:
            list[str]: A list containing the import statement and the method
                call statement, for use in Snakemake run block.
        """
        signature = ", ".join(f"{arg}" for arg in self.args)
        # The comma after the last element is mandatory for the structure of rule onces it's used in yaml.dump
        return [
            f"import {self.rel_import}\n",
            f"{self.rel_import}.{self.method_name}({signature})\n",
        ]


@define
class StepDefinition:
    """Represents a Snakemake-renderable step definition in a dewret workflow.

    Attributes:
        name (str): The name of the step.
        run (str): The run block definition for the step.
        params (List[str]): The parameter definitions for the step.
        output (list[str]: The output definition for the step.
        input (List[str]): The input definitions for the step.

    Methods:
        from_step(cls, step: Step) -> "StepDefinition": Constructs a StepDefinition
            object from a Step object, extracting step information and components
            from the step and converting them to Snakemake format.
        render(self) -> dict[str, MainTypes]: Renders the step definition as a dictionary
            suitable for use in Snakemake workflows.
    """

    name: str
    run: list[str]
    params: list[str]
    output: list[str]
    input: list[str]

    @classmethod
    def from_step(cls, step: BaseStep) -> "StepDefinition":
        """Constructs a StepDefinition object from a Step.

        Args:
            step (Step): The Step object from which step information and components
                are extracted.

        Returns:
            StepDefinition: A StepDefinition object containing the converted step
                information and components.
        """
        input_block = InputDefinition.from_step(step).render()
        run_block = RunDefinition.from_task(step.task).render()
        output_block = OutputDefinition.from_step(step).render()
        return cls(
            name=step.name,
            run=run_block,
            params=input_block["params"],
            input=input_block["inputs"],
            output=output_block,
        )

    def render(self) -> dict[str, MainTypes]:
        """Renders the step definition as a dictionary.

        Returns:
            dict[str, MainTypes]: A dictionary containing the components of the step
                definition, for use in Snakemake workflows.
        """
        return {
            "run": self.run,
            "input": self.input,
            "params": self.params,
            "output": self.output,
        }


# TODO: Find out why the yaml.dump scrambles the order of the rules
# TODO: Add a rule all: with input definition the last outputed file


@define
class WorkflowDefinition:
    """Represents a Snakemake-renderable workflow definition from a dewret workflow.

    Attributes:
        steps (List[StepDefinition]): A list of StepDefinition objects representing the steps
            in the workflow.

    Methods:
        from_workflow(cls, workflow: Workflow) -> "WorkflowDefinition": Constructs a WorkflowDefinition
            object from a Workflow object, converting its steps to StepDefinition objects.

        render(self) -> dict[str, MainTypes]: Renders the workflow definition as a dictionary
            containing Snakemake rules, for use in Snakemake workflows.
    """

    steps: list[StepDefinition]

    # TODO: Add a "rule all" definition
    # In order for snakemake to execute multiple connected rules
    # it needs a rule to define which is the target rule that connects all the multiple rules
    @classmethod
    def from_workflow(cls, workflow: Workflow) -> "WorkflowDefinition":
        """Creates a WorkflowDefinition object from a Workflow object.

        Args:
            workflow (Workflow): The workflow to be rendered.

        Returns:
            str: WorkflowDefinition with definited steps.
        """
        return cls(steps=[StepDefinition.from_step(step) for step in workflow.steps])

    def render(self) -> dict[str, MainTypes]:
        """Render the WorkflowDefinition.

        Returns:
            dict[str, MainTypes]: A dictionary containing the components of the Workflow
                definition, for use in Snakemake workflows.
        """
        return {
            f"rule {step.name.replace('-', '_')}": step.render() for step in self.steps
        }


def raw_render(workflow: Workflow) -> dict[str, MainTypes]:
    """Render the workflow as a Snakemake (SMK) string.

    This function converts a Workflow object into a object containing snakemake rules.

    Args:
        workflow (Workflow): The workflow to be rendered.

    Returns:
        dict[str, MainTypes]: A dictionary containing the components of the Workflow
            definition, for use in Snakemake workflows.
    """
    return WorkflowDefinition.from_workflow(workflow).render()


def render(workflow: Workflow) -> dict[str, typing.Any]:
    """Render the workflow as a Snakemake (SMK) string.

    This function converts a Workflow object into a Snakemake-compatible yaml.

    Args:
        workflow (Workflow): The workflow to be rendered.

    Returns:
        str: A Snakemake-compatible yaml representation of the workflow.
    """
    trans_table = str.maketrans(
        {
            "-": "   ",
            "'": "",
            "[": "",
            "]": "",
        }
    )

    return base_render(
        workflow,
        lambda workflow: yaml.dump(
            WorkflowDefinition.from_workflow(workflow).render(), indent=4
        ).translate(trans_table),
    )
