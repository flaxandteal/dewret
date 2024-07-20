"""Testing example renderer.

'Friendly render', outputting human-readable descriptions.
"""

from textwrap import indent
from typing import Unpack, TypedDict, Any
from dataclasses import dataclass
from contextvars import ContextVar

from dewret.utils import RawType
from dewret.workflow import Workflow, Step, NestedStep

from extra import JUMP

class FrenderRendererConfiguration(TypedDict):
    allow_complex_types: bool

CONFIGURATION: ContextVar[FrenderRendererConfiguration] = ContextVar("configuration")
CONFIGURATION.set({
    "allow_complex_types": True
})

@dataclass
class NestedStepDefinition:
    name: str
    subworkflow_name: str

    @classmethod
    def from_nested_step(cls, nested_step: NestedStep):
        return cls(
            name=nested_step.name,
            subworkflow_name=nested_step.subworkflow.name
        )

    def render(self):
        return \
f"""
A portal called {self.name} to another workflow,
whose name is {self.subworkflow_name}
"""

@dataclass
class StepDefinition:
    name: str

    @classmethod
    def from_step(cls, step: Step):
        return cls(
            name=step.name
        )

    def render(self):
        return \
f"""
Something called {self.name}
"""


@dataclass
class WorkflowDefinition:
    name: str
    steps: list[StepDefinition | NestedStepDefinition]

    @classmethod
    def from_workflow(cls, workflow: Workflow):
        steps = []
        for step in workflow.steps:
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

    def render(self):
        return \
f"""
I found a workflow called {self.name}.
It has {len(self.steps)} steps!
They are:
{"\n".join("* " + indent(step.render(), "  ")[3:] for step in self.steps)}
It probably got made with JUMP={JUMP}
"""

def render_raw(
    workflow: Workflow, **kwargs: Unpack[FrenderRendererConfiguration]
) -> str | tuple[str, dict[str, str]]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.
        **kwargs: additional configuration arguments - these should match CWLRendererConfiguration.

    Returns:
        Reduced form as a native Python dict structure for
        serialization.
    """
    CONFIGURATION.get().update(kwargs)
    primary_workflow = WorkflowDefinition.from_workflow(workflow).render()
    subworkflows = {}
    for step in workflow.steps:
        if isinstance(step, NestedStep):
            subworkflows[step.name] = WorkflowDefinition.from_workflow(
                step.subworkflow
            ).render()

    if subworkflows:
        return primary_workflow, subworkflows

    return primary_workflow