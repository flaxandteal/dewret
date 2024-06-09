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
# Question: I've seen Phil abstract methods like these into classless helper methods - why? 
def generate_signature_from_method(func: Lazy) -> list[str]:
    """Snakemake-renderable internal reference."""
    sign = inspect.signature(func)

    return [
        f"{param_name}=input.{param_name}" 
        for param_name, param in sign.parameters.items()
    ]

@define
class OutputDefinition:
    """Snakemake-renderable run block from a dewret workflow step."""
    output_file: str
    rel_import: str
    args: list[str]

    
    # TODO: Refactor: Error Handling and better structure
    @classmethod
    def from_task(cls, task: Task) -> "RunDefinition":
        source_file = inspect.getsourcefile(task.target)
        relative_path = os.path.relpath(source_file)
        rel_import = f"import {relative_path}"

        args = generate_signature_from_method(task.target)
        return cls(
            method_name=task.name,
            rel_import=rel_import,
            args=args
        )
    
    def render(self) -> tuple[str | RawType]:
        signature = ", ".join(f"{arg}" for arg in self.args)
        return (
            self.rel_import,
            f"{self.method_name}({signature})",
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
        source_file = inspect.getsourcefile(task.target)
        relative_path = os.path.relpath(source_file)
        rel_import = f"import {relative_path}"

        args = generate_signature_from_method(task.target)
        return cls(
            method_name=task.name,
            rel_import=rel_import,
            args=args
        )
    
    def render(self) -> tuple[str | RawType]:
        signature = ", ".join(f"{arg}" for arg in self.args)
        return (
            self.rel_import,
            f"{self.method_name}({signature})",
        )

@define
class StepDefinition:
    """Snakemake-renderable rule from a dewret workflow step."""

    name: str
    run: str
    params: list[str]
    # in_: Mapping[str, ReferenceDefinition | Raw]
    in_: list[str]

    # TODO: Refactor: Use custom made RunDefinition to populate the run block of the snakefile
    # TODO: Refactor: Use custom made InputDefinition to populate the input block of the snakefile
    # TODO: Refactor: Use custom made ParamsDefinition to populate the params block of the snakefile
    # TODO: Refactor: Use custom made RunDefinition to populate the run block of the snakefile
    # TODO: Fix: TypeError: RunDefinition() takes no arguments

    @classmethod
    def from_step(cls, step: Step, inxed_steps: dict[str, Step]) -> "StepDefinition":
        params = []
        inputs = []
        for key, param in step.arguments.items():
            if isinstance(param, Reference):
                ref = ReferenceDefinition.from_reference(param).render()
                input = f"{key}=rules.{ref.replace("-","_").replace("/out", ".output")}.file"
                inputs.append(input)
            else:
                param = f"{key}={to_snakemake_type(param)}"
                params.append(param)

        run_block = RunDefinition.from_task(step.task).render()
        return cls(
            name=step.name, 
            run=run_block, 
            params=params, 
            in_=inputs
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
            "output": "\nfile=input.output_file",
        }


@define
class WorkflowDefinition:
    steps: list[StepDefinition]

    @classmethod
    def from_workflow(cls, workflow: Workflow) -> "WorkflowDefinition":
        return cls(
            steps=[
                StepDefinition.from_step(step, workflow._indexed_steps)
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
    print(WorkflowDefinition.from_workflow(workflow).render())
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
