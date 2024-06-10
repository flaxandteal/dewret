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

from attrs import define
from dewret.workflow import Lazy
from dewret.workflow import Reference, Raw, Workflow, Step, Task
from dewret.utils import RawType
from collections.abc  import ItemsView

# TODO: Write: Better description of classes.

# TODO: Define: Inputschema


@define
class ReferenceDefinition:
    """Snakemake-renderable internal reference."""

    source: str

    @classmethod
    def from_reference(cls, ref: Reference) -> "ReferenceDefinition":
        """Build from a `Reference`.

        Converts a `dewret.workflow.Reference` into a Snakemake-rendering object.

        Args:
            ref: reference to convert.
        """
        return cls(source=ref.name)

    def render(self) -> str:
        return self.source

# TODO: Refactor: better way to handle the types.
def to_snakemake_type(param: Raw) -> str:
    """Snakemake-renderable internal reference."""
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
        raise TypeError(f"Cannot render complex type ({typ}) to JSON")
    
# TODO: Refactor: better way to handle the types.
# Question: I've seen Phil abstract methods like these into classless helper methods - why? It happens in go aswell
# but I always thought that was because there's no classes
def get_method_args(func: Lazy) -> ItemsView[str, inspect.Parameter]:
    """Snakemake-renderable internal reference."""
    args = inspect.signature(func)

    return args

def get_method_rel_path(func: Lazy) -> any:
    """Snakemake-renderable internal reference."""
    source_file = inspect.getsourcefile(func)
    relative_path = os.path.relpath(source_file, start=os.getcwd())
    module_name = os.path.splitext(relative_path)[0].replace(os.path.sep, '.')

    return module_name

@define
class InputDefinition:
    """Snakemake-renderable run block from a dewret workflow step."""
    inputs: str
    params: str
    
    # TODO: Refactor: Error Handling and better structure
    @classmethod
    def from_step(cls, step: Step) -> "InputDefinition":
        params = []
        inputs = []
        for key, param in step.arguments.items():
            if isinstance(param, Reference):
                ref = ReferenceDefinition.from_reference(param).render()
                input = f"{key}=rules.{ref.replace("-","_").replace("/out", ".output")}.output_file"
                inputs.append(input)
                param = f"{key}=input.{key},"
                params.append(param)
            else:
                param = f"{key}={to_snakemake_type(param)},"
                params.append(param)

        # # Check if the list is not empty
        if params:
            params[len(params)-1] = params[len(params)-1].replace(",", "")

        return cls(
            inputs=inputs,
            params=params
        )
    
    def render(self) -> dict[str, RawType]:
        return {
            "inputs": self.inputs,
            "params": self.params
        }

@define
class OutputDefinition:
    """Snakemake-renderable run block from a dewret workflow step."""
    output_file: str
    # I need a way to find the output_file, for now I assume it'll be in the method signature

    
    # TODO: Refactor: Error Handling and better structure
    @classmethod
    def from_step(cls, step: Step) -> "OutputDefinition":
        args = step.arguments["output_file"]
        args = to_snakemake_type(args)
        return cls(
            output_file=args
        )
    
    def render(self) -> tuple[str | RawType]:
        return (
            f"output_file={self.output_file}",
        )

@define
class RunDefinition:
    """Snakemake-renderable run block from a dewret workflow step."""
    method_name: str
    rel_import: str
    args: list[str]

    
    # TODO: Refactor: Error Handling and better structure
    @classmethod
    def from_task(cls, task: Task) -> "RunDefinition":
        relative_path = get_method_rel_path(task.target)
        rel_import = f"{relative_path}"

        # TODO: Make sure to use Paramdefinitions for getting the parameters
        args = get_method_args(task.target)
        signature = [
            f"{param_name}=params.{param_name}" 
            for param_name, param in args.parameters.items()
        ]

        return cls(
            method_name=task.name,
            rel_import=rel_import,
            args=signature
        )
    
    def render(self) -> tuple[str | RawType]:
        signature = ", ".join(f"{arg}" for arg in self.args)
        return (
            f"import {self.rel_import}\n",
            f"{self.rel_import}.{self.method_name}({signature})\n",
        )

@define
class StepDefinition:
    """Snakemake-renderable rule from a dewret workflow step."""

    name: str
    run: str
    params: list[str]
    output: tuple[RawType]
    # in_: Mapping[str, ReferenceDefinition | Raw]
    in_: list[str]

    # TODO: Refactor: Use custom made RunDefinition to populate the run block of the snakefile
    # TODO: Refactor: Use custom made InputDefinition to populate the input block of the snakefile
    # TODO: Refactor: Use custom made ParamsDefinition to populate the params block of the snakefile
    # TODO: Refactor: Use custom made RunDefinition to populate the run block of the snakefile
    # TODO: Fix: TypeError: RunDefinition() takes no arguments

    @classmethod
    def from_step(cls, step: Step) -> "StepDefinition":                
        input_block = InputDefinition.from_step(step).render()
        run_block = RunDefinition.from_task(step.task).render()
        output_block = OutputDefinition.from_step(step).render()
        return cls(
            name=step.name, 
            run=run_block, 
            params=input_block["params"], 
            in_=input_block["inputs"],
            output=output_block
        )

    # TODO: Fix: Params must be comma delimited or they get errors
    # TODO: Fix: New line after output causes snakemake errors
    # TODO: Fix: You can't call parameters from params block in output block
    # TODO: Refactor: Create OutputDefinition to populate the output block of the snakefile
    def render(self) -> dict[str, RawType]:
        return {
            "run": self.run,
            "input": self.in_,
            "params": self.params,
            "output": self.output,
        }


# TODO: Fix: Find out why the order of the rules is scrambled
# TODO: Fix: Add a rule all: with input definition the last outputed file
@define
class WorkflowDefinition:
    steps: list[StepDefinition]

    # Question: Is it more abstract if I define the different blocks in the Workflow definition or the steps definition?
    @classmethod
    def from_workflow(cls, workflow: Workflow) -> "WorkflowDefinition":
        return cls(
            steps=[
                StepDefinition.from_step(step)
                for step in workflow.steps
            ]
        )

    def render(self) -> dict[str, RawType]:
        return {
            f"rule {step.name.replace("-", "_")}": step.render() for step in self.steps
        }


# Takes a dewret workflow object and creates a workflow based on the workflow language.
def render(workflow: Workflow) -> str:
    """Render to a SMK string."""
    trans_table = str.maketrans(
        {
            "-": "   ",
            "'": "",
            "[": "",
            "]": "",
        }
    )
    print(
        yaml.dump(
            WorkflowDefinition.from_workflow(workflow).render(), indent=4
        ).translate(trans_table).replace("!!python/tuple", "")
    )
    return yaml.dump(
        WorkflowDefinition.from_workflow(workflow).render(), indent=4
    ).translate(trans_table).replace("!!python/tuple", "")
