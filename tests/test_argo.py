"""Verify Argo Workflows output is OK."""

import yaml
import pytest
from attr import define
from dataclasses import dataclass
from dewret.tasks import construct, task, workflow
from dewret.core import set_configuration
from dewret.renderers.argo import render
from dewret.utils import hasher
from dewret.workflow import param

from ._lib.extra import (
    pi,
    increment,
    double,
    sum,
    triple_and_one,
)


@define
class SplitResult:
    """Test class showing two named values, using attrs."""

    first: int
    second: float


@dataclass
class SplitResultDataclass:
    """Test class showing two named values, using dataclasses."""

    first: int
    second: float


@task()
def split() -> SplitResult:
    """Create a split result with two fields."""
    return SplitResult(first=1, second=2)


@task()
def split_into_dataclass() -> SplitResultDataclass:
    """Create a result with two fields."""
    return SplitResultDataclass(first=1, second=2)


@task()
def combine(left: int, right: float) -> float:
    """Sum two values."""
    return left + right


def test_basic_argo() -> None:
    """Check whether we can produce a simple Argo Workflow.

    Single task with no inputs, verifying one container template
    and one DAG task.
    """
    result = pi()
    workflow = construct(result)
    rendered = render(workflow)["__root__"]
    hsh = hasher(("pi",))

    assert rendered["apiVersion"] == "argoproj.io/v1alpha1"
    assert rendered["kind"] == "Workflow"
    assert rendered["spec"]["entrypoint"] == "main"

    templates = rendered["spec"]["templates"]
    # Should have DAG template + 1 container template
    assert len(templates) == 2

    dag_template = templates[0]
    assert dag_template["name"] == "main"
    assert len(dag_template["dag"]["tasks"]) == 1

    dag_task = dag_template["dag"]["tasks"][0]
    assert dag_task["name"] == f"pi-{hsh}"
    assert dag_task["template"] == f"pi-{hsh}"
    assert "dependencies" not in dag_task

    container_template = templates[1]
    assert container_template["name"] == f"pi-{hsh}"
    assert container_template["container"]["image"] == "python:3.9"
    assert container_template["outputs"]["parameters"][0]["name"] == "out"


def test_two_step_dag() -> None:
    """Check whether step B depends on step A's output.

    Verifies dependencies and Argo template variable references.
    """
    result = double(num=increment(num=3))
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]

    dag_template = rendered["spec"]["templates"][0]
    tasks = {t["name"]: t for t in dag_template["dag"]["tasks"]}

    # increment has no dependencies
    assert "dependencies" not in tasks["increment-1"]

    # double depends on increment
    assert tasks["double-1"]["dependencies"] == ["increment-1"]

    # double's argument references increment's output
    double_args = {
        p["name"]: p["value"] for p in tasks["double-1"]["arguments"]["parameters"]
    }
    assert double_args["num"] == "{{tasks.increment-1.outputs.parameters.out}}"


def test_workflow_parameters() -> None:
    """Check that workflow-level parameters end up in spec.arguments.parameters."""
    my_param = param("my_param", typ=int)

    result = increment(num=my_param)
    workflow = construct(result)
    rendered = render(workflow)["__root__"]

    # Should have workflow-level parameters
    assert "arguments" in rendered["spec"]
    params = rendered["spec"]["arguments"]["parameters"]
    param_names = [p["name"] for p in params]
    assert "my-param" in param_names

    # The DAG task should reference the workflow parameter
    dag_template = rendered["spec"]["templates"][0]
    task_args = {
        p["name"]: p["value"]
        for p in dag_template["dag"]["tasks"][0]["arguments"]["parameters"]
    }
    assert task_args["num"] == "{{workflow.parameters.my-param}}"


def test_workflow_parameter_with_default() -> None:
    """Check that parameters with defaults include them."""
    result = increment(num=42)
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]

    params = rendered["spec"]["arguments"]["parameters"]
    assert len(params) == 1
    assert params[0]["value"] == "42"


def test_configuration_overrides() -> None:
    """Verify kind, namespace, and image overrides."""
    result = pi()
    workflow = construct(result)
    rendered = render(
        workflow,
        kind="WorkflowTemplate",
        namespace="my-namespace",
        image="python:3.11-slim",
    )["__root__"]

    assert rendered["kind"] == "WorkflowTemplate"
    assert rendered["metadata"]["namespace"] == "my-namespace"

    # Container template should use overridden image
    container_template = rendered["spec"]["templates"][1]
    assert container_template["container"]["image"] == "python:3.11-slim"


def test_multi_step_dag_dependencies() -> None:
    """Check a fan-in pattern: two steps feed into a third."""
    result = sum(left=double(num=increment(num=23)), right=increment(num=23))
    workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]

    dag_template = rendered["spec"]["templates"][0]
    tasks = {t["name"]: t for t in dag_template["dag"]["tasks"]}

    # increment-1 has no deps
    assert "dependencies" not in tasks["increment-1"]

    # double-1 depends on increment-1
    assert tasks["double-1"]["dependencies"] == ["increment-1"]

    # sum-1 depends on both double-1 and increment-1
    sum_deps = sorted(tasks["sum-1"]["dependencies"])
    assert sum_deps == ["double-1", "increment-1"]


def test_nested_workflow() -> None:
    """Check that a @workflow() subworkflow produces a separate Argo resource."""
    my_param = param("num", typ=int)
    result = increment(num=triple_and_one(num=my_param))
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow)

    # Should have __root__ plus one subworkflow
    assert "__root__" in subworkflows
    assert len(subworkflows) > 1

    root = subworkflows["__root__"]
    assert root["apiVersion"] == "argoproj.io/v1alpha1"

    # The subworkflow should also be a valid Argo resource
    sub_keys = [k for k in subworkflows if k != "__root__"]
    assert len(sub_keys) == 1
    sub = subworkflows[sub_keys[0]]
    assert sub["apiVersion"] == "argoproj.io/v1alpha1"
    assert sub["spec"]["entrypoint"] == "main"

    # Subworkflow should have its own DAG with steps
    sub_dag = sub["spec"]["templates"][0]
    assert sub_dag["name"] == "main"
    assert len(sub_dag["dag"]["tasks"]) > 0


def test_no_namespace_by_default() -> None:
    """Namespace should be absent from metadata when not configured."""
    result = pi()
    workflow = construct(result)
    rendered = render(workflow)["__root__"]
    assert "namespace" not in rendered["metadata"]


def test_structured_output_dataclass() -> None:
    """Dataclass return type should generate per-field output parameters."""
    result = split_into_dataclass()
    wf = construct(result, simplify_ids=True)
    rendered = render(wf)["__root__"]

    templates = {t["name"]: t for t in rendered["spec"]["templates"] if "container" in t}
    tpl = templates["split-into-dataclass-1"]
    outputs = {p["name"]: p for p in tpl["outputs"]["parameters"]}

    assert set(outputs.keys()) == {"first", "second"}
    assert outputs["first"]["valueFrom"]["path"] == "/tmp/dewret-out/first"
    assert outputs["second"]["valueFrom"]["path"] == "/tmp/dewret-out/second"


def test_structured_output_attrs() -> None:
    """Attrs return type should generate per-field output parameters."""
    result = split()
    wf = construct(result, simplify_ids=True)
    rendered = render(wf)["__root__"]

    templates = {t["name"]: t for t in rendered["spec"]["templates"] if "container" in t}
    tpl = templates["split-1"]
    outputs = {p["name"]: p for p in tpl["outputs"]["parameters"]}

    assert set(outputs.keys()) == {"first", "second"}
    assert outputs["first"]["valueFrom"]["path"] == "/tmp/dewret-out/first"
    assert outputs["second"]["valueFrom"]["path"] == "/tmp/dewret-out/second"


def test_structured_output_field_references() -> None:
    """Field references to structured outputs should use field names in Argo references."""
    result = combine(left=split().first, right=split().second)
    wf = construct(result, simplify_ids=True)
    rendered = render(wf)["__root__"]

    dag_template = rendered["spec"]["templates"][0]
    tasks = {t["name"]: t for t in dag_template["dag"]["tasks"]}

    combine_args = {
        p["name"]: p["value"] for p in tasks["combine-1"]["arguments"]["parameters"]
    }
    assert combine_args["left"] == "{{tasks.split-1.outputs.parameters.first}}"
    assert combine_args["right"] == "{{tasks.split-1.outputs.parameters.second}}"


def test_structured_output_custom_override() -> None:
    """Custom task_configs outputs should override type-derived outputs."""
    custom_outputs = {
        "parameters": [
            {"name": "custom", "valueFrom": {"path": "/custom/path"}}
        ]
    }
    result = split_into_dataclass()
    wf = construct(result, simplify_ids=True)
    rendered = render(
        wf,
        task_configs={"split_into_dataclass": {"outputs": custom_outputs}},
    )["__root__"]

    templates = {t["name"]: t for t in rendered["spec"]["templates"] if "container" in t}
    tpl = templates["split-into-dataclass-1"]
    assert tpl["outputs"] == custom_outputs
