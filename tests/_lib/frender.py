"""Testing example renderer.

'Friendly render', outputting human-readable descriptions.
"""

from textwrap import indent
from typing import Unpack, TypedDict
from dataclasses import dataclass

from dewret.core import set_render_configuration
from dewret.workflow import Workflow, Step, NestedStep
from dewret.render import base_render

from .extra import JUMP


class FrenderRendererConfiguration(TypedDict):
    allow_complex_types: bool


def default_config() -> FrenderRendererConfiguration:
    return FrenderRendererConfiguration({"allow_complex_types": True})


@dataclass
class NestedStepDefinition:
    name: str
    subworkflow_name: str

    @classmethod
    def from_nested_step(cls, nested_step: NestedStep) -> "NestedStepDefinition":
        return cls(name=nested_step.name, subworkflow_name=nested_step.subworkflow.name)

    def render(self) -> str:
        return f"""
A portal called {self.name} to another workflow,
whose name is {self.subworkflow_name}
"""


@dataclass
class StepDefinition:
    name: str

    @classmethod
    def from_step(cls, step: Step) -> "StepDefinition":
        return cls(name=step.name)

    def render(self) -> str:
        return f"""
Something called {self.name}
"""


@dataclass
class WorkflowDefinition:
    name: str
    steps: list[StepDefinition | NestedStepDefinition]

    @classmethod
    def from_workflow(cls, workflow: Workflow) -> "WorkflowDefinition":
        steps: list[StepDefinition | NestedStepDefinition] = []
        for step in workflow.indexed_steps.values():
            if isinstance(step, Step):
                steps.append(StepDefinition.from_step(step))
            elif isinstance(step, NestedStep):
                steps.append(NestedStepDefinition.from_nested_step(step))
            else:
                raise RuntimeError(f"Unrecognised step type: {type(step)}")

        try:
            name = workflow.name
        except NameError:
            name = "Work Doe"
        return cls(name=name, steps=steps)

    def render(self) -> str:
        steps = "\n".join("* " + indent(step.render(), "  ")[3:] for step in self.steps)
        return f"""
I found a workflow called {self.name}.
It has {len(self.steps)} steps!
They are:
{steps}
It probably got made with JUMP={JUMP}
"""


def render_raw(
    workflow: Workflow, **kwargs: Unpack[FrenderRendererConfiguration]
) -> dict[str, str]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.
        **kwargs: additional configuration arguments - these should match CWLRendererConfiguration.

    Returns:
        Reduced form as a native Python dict structure for
        serialization.
    """
    # TODO: work out how to handle these hints correctly.
    set_render_configuration(kwargs)  # type: ignore
    return base_render(
        workflow, lambda workflow: WorkflowDefinition.from_workflow(workflow).render()
    )
