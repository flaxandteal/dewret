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

"""CWL Renderer.

Outputs a [Common Workflow Language](https://www.commonwl.org/) representation of the
current workflow.
"""

from attrs import define
from collections.abc import Mapping

from dewret.workflow import Reference, Raw, Workflow, Step, Task, StepReference
from dewret.tasks import run
from dewret.utils import RawType

@define
class ReferenceDefinition:
    """CWL-renderable internal reference.

    Normally points to a value or a step.
    """
    source: str

    @classmethod
    def from_reference(cls, ref: Reference) -> "ReferenceDefinition":
        """Build from a `Reference`.

        Converts a `dewret.workflow.Reference` into a CWL-rendering object.

        Args:
            ref: reference to convert.
        """
        return cls(source=ref.name)

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        return {
            "source": self.source
        }

@define
class StepDefinition:
    """CWL-renderable step.

    Coerces the dewret structure of a step into that
    needed for valid CWL.

    Attributes:
        name: identifier to call this step by.
        run: task to execute for this step.
        in_: inputs from values or other steps.
    """

    name: str
    run: str
    in_: Mapping[str, ReferenceDefinition | Raw]

    @classmethod
    def from_step(cls, step: Step) -> "StepDefinition":
        """Build from a `Step`.

        Converts a `dewret.workflow.Step` into a CWL-rendering object.

        Args:
            step: step to convert.
        """
        return cls(
            name=step.name,
            run=step.task.name,
            in_={
                key: (
                    ReferenceDefinition.from_reference(param)
                    if isinstance(param, Reference) else
                    param
                ) for key, param in step.parameters.items()
            }
        )

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        return {
            "run": self.run,
            "in": {
                key: (
                    ref.render()
                    if isinstance(ref, ReferenceDefinition) else
                    {"default": ref.value}
                ) for key, ref in self.in_.items()
            },
            "out": ["out"]
        }

@define
class OutputsDefinition:
    """CWL-renderable set of workflow outputs.

    Turns dewret results into a CWL output block.

    Attributes:
        outputs: sequence of results from a workflow.
    """

    outputs: dict[str, "OutputReferenceDefinition"]

    @define
    class OutputReferenceDefinition:
        """CWL-renderable reference to a specific output.

        Attributes:
            vartype: type of variable
            name: fully-qualified name of the referenced step output.
        """
        vartype: str
        name: str

    @classmethod
    def from_results(cls, results: dict[str, StepReference]) -> "OutputsDefinition":
        """Takes a mapping of results into a CWL structure.

        [TODO] For now, it assumes the output type is a string.

        Returns:
            CWL-like structure representing all workflow outputs.
        """
        return cls(
            outputs={
                key: cls.OutputReferenceDefinition(
                    vartype="string", # TODO: pull from signature
                    name=result.name
                ) for key, result in results.items()
            }
        )

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        return {
            key: {
                "type": output.vartype,
                "outputSource": output.name
            } for key, output in self.outputs.items()
        }

@define
class WorkflowDefinition:
    """CWL-renderable workflow.

    Coerces the dewret structure of a workflow into that
    needed for valid CWL.

    Attributes:
        steps: sequence of steps in the workflow.
    """

    steps: list[StepDefinition]
    outputs: OutputsDefinition

    @classmethod
    def from_workflow(cls, workflow: Workflow) -> "WorkflowDefinition":
        """Build from a `Workflow`.

        Converts a `dewret.workflow.Workflow` into a CWL-rendering object.

        Args:
            workflow: workflow to convert.
        """
        return cls(
            steps=[
                StepDefinition.from_step(step)
                for step in workflow.steps
            ],
            outputs=OutputsDefinition.from_results({
                "out": workflow.result
            } if workflow.result else {})
        )

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        return {
            "cwlVersion": 1.2,
            "class": "Workflow",
            "outputs": self.outputs.render(),
            "steps": {
                step.name: step.render()
                for step in self.steps
            }
        }

def render(workflow: Workflow) -> dict[str, RawType]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.

    Returns:
        Reduced form as a native Python dict structure for
        serialization.
    """
    return WorkflowDefinition.from_workflow(workflow).render()
