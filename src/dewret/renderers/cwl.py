from attrs import define
from collections.abc import Mapping

from dewret.workflow import Reference, Raw, Workflow, Step, Task
from dewret.tasks import run

@define
class ReferenceDefinition:
    @classmethod
    def from_reference(cls, ref: Reference) -> "ReferenceDefinition":
        return cls()

@define
class StepDefinition:
    id: str
    run: str
    in_: Mapping[str, ReferenceDefinition | Raw]

    @classmethod
    def from_step(cls, step: Step):
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

    def render(self):
        return {
            "run": self.run,
            "in": {
                key: (
                    ref.render()
                    if isinstance(ref, ReferenceDefinition) else
                    {"default": ref.value}
                ) for key, ref in self.in_.items()
            }
        }

@define
class WorkflowDefinition:
    steps: list[StepDefinition]

    @classmethod
    def from_workflow(cls, workflow: Workflow):
        return cls(
            steps=[
                StepDefinition.from_step(step)
                for step in workflow.steps
            ]
        )

    def render(self):
        return {
            "cwlVersion": 1.2,
            "class": "Workflow",
            "steps": {
                step.id: step.render()
                for step in self.steps
            }
        }

def render(task: Task):
    output = run(task)
    workflow = output.__workflow__
    return WorkflowDefinition.from_workflow(workflow).render()
