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

from dewret.workflow import Reference, Raw, Workflow, Step, Task
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
        return cls(source=str(ref))

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
        id: identifier to call this step by.
        run: task to execute for this step.
        in_: inputs from values or other steps.
    """

    id: str
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
            id=step.id,
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
class WorkflowDefinition:
    """CWL-renderable workflow.

    Coerces the dewret structure of a workflow into that
    needed for valid CWL.

    Attributes:
        steps: sequence of steps in the workflow.
    """

    steps: list[StepDefinition]

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
            ]
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
            "steps": {
                step.id: step.render()
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
