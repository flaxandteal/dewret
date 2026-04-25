#!/usr/bin/env python3
"""Minimal Argo Workflows example using dewret.

Demonstrates a two-step workflow (simulate -> post-process) rendered
to native Argo YAML (apiVersion: argoproj.io/v1alpha1).
"""

import yaml
from dewret.tasks import task, construct
from dewret.workflow import param
from dewret.renderers.argo import render


@task()
def simulate(duration: int, model: str) -> float:
    """Run an EnergyPlus simulation and return a temperature."""
    return 21.5


@task()
def post_process(temperature: float, threshold: float) -> bool:
    """Check whether the simulation result exceeds a threshold."""
    return temperature > threshold


def main() -> None:
    duration = param("duration", default=480, typ=int)
    model = param("model", default="dollhouse_6zone", typ=str)
    threshold = param("threshold", default=22.0, typ=float)

    temp = simulate(duration=duration, model=model)
    result = post_process(temperature=temp, threshold=threshold)

    workflow = construct(result, simplify_ids=True)

    rendered = render(
        workflow,
        kind="WorkflowTemplate",
        namespace="dusk",
        image="ghcr.io/example/energyplus:latest",
        command=["python3", "-c"],
    )

    for name, doc in rendered.items():
        if name != "__root__":
            print(f"--- # subworkflow: {name}")
        print(yaml.safe_dump(doc, default_flow_style=False, sort_keys=False))


if __name__ == "__main__":
    main()
