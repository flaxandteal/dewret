# Copyright 2024- Flax & Teal Limited. All Rights Reserved.
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

"""Argo Workflows Renderer.

Outputs a native [Argo Workflows](https://argoproj.github.io/workflows/) representation
of the current workflow. Produces ``apiVersion: argoproj.io/v1alpha1`` resources directly,
with zero external dependencies beyond dewret itself.
"""

import re
from attrs import define, has as attrs_has, fields as attrs_fields
from dataclasses import is_dataclass, fields as dataclass_fields
from inspect import isclass
from typing import (
    TypedDict,
    NotRequired,
    Any,
    Unpack,
)

from sympy import Basic

from dewret.core import (
    Raw,
    RawType,
)
from dewret.workflow import (
    FactoryCall,
    Workflow,
    BaseStep,
    NestedStep,
    StepReference,
    ParameterReference,
    expr_to_references,
)
from dewret.utils import (
    firm_to_raw,
    flatten_if_set,
    Unset,
)
from dewret.render import base_render
from dewret.core import Reference, set_render_configuration


class TaskTemplateConfig(TypedDict):
    """Per-task template overrides.

    Keyed by the task function name in ``task_configs``. Any field
    set here overrides the corresponding default from the top-level
    renderer configuration for that specific step's container template.

    Attributes:
        image: container image for this task.
        command: entrypoint command.
        args: command arguments (replaces default ``[task_name]``).
        env: environment variables for this container.
        resources: resource requests/limits.
        volume_mounts: volumeMounts for this container.
        sidecars: sidecar containers attached to this template.
        outputs: custom outputs dict (replaces default parameter output).
        image_pull_policy: imagePullPolicy for the container.
    """

    image: NotRequired[str]
    command: NotRequired[list[str]]
    args: NotRequired[list[str]]
    env: NotRequired[list[dict[str, Any]]]
    resources: NotRequired[dict[str, Any]]
    volume_mounts: NotRequired[list[dict[str, Any]]]
    sidecars: NotRequired[list[dict[str, Any]]]
    outputs: NotRequired[dict[str, Any]]
    image_pull_policy: NotRequired[str]


class ArgoRendererConfiguration(TypedDict):
    """Configuration for the Argo renderer.

    Attributes:
        kind: "Workflow" or "WorkflowTemplate", default "Workflow".
        namespace: metadata.namespace for the resource.
        generate_name: use metadata.generateName instead of metadata.name.
        image: default container image, default "python:3.9".
        command: default command, default ["python3"].
        env: env vars added to every template.
        resources: default resource requests/limits.
        volumes: spec-level volume definitions (emptyDir, etc.).
        sidecars: default sidecar container specs (applied to all templates).
        task_configs: per-task template overrides, keyed by function name.
    """

    kind: NotRequired[str]
    name: NotRequired[str]
    namespace: NotRequired[str]
    generate_name: NotRequired[str]
    image: NotRequired[str]
    command: NotRequired[list[str]]
    env: NotRequired[list[dict[str, Any]]]
    resources: NotRequired[dict[str, Any]]
    volumes: NotRequired[list[dict[str, Any]]]
    sidecars: NotRequired[list[dict[str, Any]]]
    task_configs: NotRequired[dict[str, TaskTemplateConfig]]


def default_config() -> ArgoRendererConfiguration:
    """Default configuration for this renderer.

    Returns: a dict with raw type structures for easy setting from YAML/JSON.
    """
    return {
        "kind": "Workflow",
        "image": "python:3.9",
        "command": ["python3"],
    }


def _k8s_name(name: str) -> str:
    """Sanitize a name for Kubernetes compatibility.

    Converts a dewret step/parameter name to a valid K8s resource name:
    lowercase alphanumeric, hyphens, dots. Must start/end alphanumeric.

    Args:
        name: the raw name.

    Returns: a K8s-safe name string.
    """
    sanitized = re.sub(r"[^a-z0-9.\-]", "-", name.lower())
    sanitized = re.sub(r"-+", "-", sanitized)
    sanitized = sanitized.strip("-.")
    return sanitized or "step"


def _with_field(ref: Any) -> str:
    """Get the field portion of a reference, defaulting to 'out'.

    Args:
        ref: a reference with a possible __field__ attribute.

    Returns: the field string or "out".
    """
    if hasattr(ref, "__field__") and ref.__field__:
        return str(ref.__field_str__)
    return "out"


def _output_fields_for_type(return_type: type | None) -> list[str]:
    """Determine output field names from a step's return type.

    If the return type is an attrs or dataclass type, returns the field names.
    Otherwise returns ``["out"]`` for the default single-output convention.

    Args:
        return_type: the step's return type annotation, or None.

    Returns: list of output field name strings.
    """
    if return_type is not None:
        if attrs_has(return_type):
            return [str(f.name) for f in attrs_fields(return_type)]
        if is_dataclass(return_type) and isclass(return_type):
            return [str(f.name) for f in dataclass_fields(return_type)]
    return ["out"]


def _render_argo_reference(ref: Any) -> str:
    """Convert a Reference into an Argo template variable string.

    Args:
        ref: a StepReference, ParameterReference, or sympy expression.

    Returns: an Argo template expression string like ``{{tasks.foo.outputs.parameters.out}}``.
    """
    if isinstance(ref, Basic):
        values = list(ref.free_symbols)
        step_syms = [sym for sym in values if isinstance(sym, StepReference)]
        param_syms = [sym for sym in values if isinstance(sym, ParameterReference)]

        if set(values) != set(step_syms) | set(param_syms):
            raise NotImplementedError(
                f"Can only build Argo expressions for step results and param results: {ref}"
            )

        # Simple case: the expression is just a single reference
        if len(values) == 1 and values[0] is ref:
            ref = values[0]
        elif step_syms or param_syms:
            raise NotImplementedError(
                "Argo does not support complex arithmetic expressions in parameter values. "
                f"Expression: {ref}"
            )

    if isinstance(ref, StepReference):
        step_name = _k8s_name(ref._.step.name)
        field = _with_field(ref)
        return f"{{{{tasks.{step_name}.outputs.parameters.{field}}}}}"
    elif isinstance(ref, ParameterReference):
        return f"{{{{workflow.parameters.{_k8s_name(ref.name)}}}}}"

    raise NotImplementedError(f"Cannot render reference type to Argo: {type(ref)}")


@define
class ReferenceDefinition:
    """Argo-renderable internal reference.

    Holds either a template expression string or a literal value.
    """

    value: str
    is_literal: bool

    @classmethod
    def from_reference(cls, ref: Reference[Any]) -> "ReferenceDefinition":
        """Build from a Reference.

        Args:
            ref: reference to convert.
        """
        return cls(value=_render_argo_reference(ref), is_literal=False)


@define
class StepDefinition:
    """Argo-renderable step.

    Produces both a container template and a DAG task for the step.

    Attributes:
        name: sanitized step name.
        task_name: the underlying task/function name.
        arguments: input arguments (references or raw values).
        dependencies: names of upstream steps this depends on.
        is_nested: whether this step calls a subworkflow.
    """

    name: str
    task_name: str
    arguments: dict[str, ReferenceDefinition | Raw]
    dependencies: list[str]
    is_nested: bool
    return_type: type | None

    @classmethod
    def from_step(cls, step: BaseStep) -> "StepDefinition":
        """Build from a BaseStep.

        Converts a dewret step into an Argo-rendering object, computing
        dependencies by inspecting argument references.

        Args:
            step: step to convert.
        """
        step_name = _k8s_name(step.name)
        is_nested = isinstance(step, NestedStep)
        task_name = step.task.name if not isinstance(step.task, Workflow) else step.name

        # Build arguments and collect dependencies
        args: dict[str, ReferenceDefinition | Raw] = {}
        deps: set[str] = set()

        for key, param in step.arguments.items():
            if isinstance(param, Reference):
                args[key] = ReferenceDefinition.from_reference(param)
                # Extract dependency from StepReferences
                _, refs = expr_to_references(param)
                for r in refs:
                    if isinstance(r, StepReference):
                        deps.add(_k8s_name(r._.step.name))
            elif isinstance(param, Basic):
                # Sympy expression — try to extract refs
                _, refs = expr_to_references(param)
                step_refs = [r for r in refs if isinstance(r, StepReference)]
                param_refs = [r for r in refs if isinstance(r, ParameterReference)]
                if len(step_refs) + len(param_refs) == 1 and len(refs) == 1:
                    args[key] = ReferenceDefinition(
                        value=_render_argo_reference(refs[0]), is_literal=False
                    )
                    for r in step_refs:
                        deps.add(_k8s_name(r._.step.name))
                elif refs:
                    raise NotImplementedError(
                        f"Complex expressions not supported in Argo renderer: {param}"
                    )
                else:
                    # Pure constant expression (no references)
                    args[key] = Raw(str(param))
            elif isinstance(param, Raw):
                args[key] = param
            else:
                args[key] = Raw(param)

        return cls(
            name=step_name,
            task_name=task_name,
            arguments=args,
            dependencies=sorted(deps),
            is_nested=is_nested,
            return_type=step.return_type,
        )

    def _get_task_config(
        self, config: ArgoRendererConfiguration
    ) -> TaskTemplateConfig:
        """Look up per-task overrides from the renderer configuration.

        Checks ``task_configs`` for the task function name.

        Args:
            config: the full renderer configuration.

        Returns: task-specific overrides or empty dict.
        """
        task_configs = config.get("task_configs", {})
        return task_configs.get(self.task_name, {})

    def render_template(self, config: ArgoRendererConfiguration) -> dict[str, RawType]:
        """Render the container template for this step.

        Merges global defaults with per-task overrides from ``task_configs``.

        Args:
            config: renderer configuration for image, command, etc.

        Returns: an Argo template dict.
        """
        tc = self._get_task_config(config)

        image = tc.get("image", config.get("image", "python:3.9"))
        command = tc.get("command", config.get("command", ["python3"]))
        args = tc.get("args", [self.task_name])
        env = tc.get("env", config.get("env"))
        resources = tc.get("resources", config.get("resources"))
        volume_mounts = tc.get("volume_mounts")
        sidecars = tc.get("sidecars", config.get("sidecars"))
        custom_outputs = tc.get("outputs")
        image_pull_policy = tc.get("image_pull_policy")

        template: dict[str, RawType] = {"name": self.name}

        # Input parameters
        if self.arguments:
            template["inputs"] = {
                "parameters": [{"name": key} for key in self.arguments]
            }

        # Container spec
        container: dict[str, RawType] = {
            "image": image,
            "command": command,
            "args": args,
        }
        if image_pull_policy:
            container["imagePullPolicy"] = image_pull_policy
        if env:
            container["env"] = env
        if resources:
            container["resources"] = resources
        if volume_mounts:
            container["volumeMounts"] = volume_mounts
        template["container"] = container

        # Sidecars
        if sidecars:
            template["sidecars"] = sidecars

        # Outputs: custom or type-aware default
        if custom_outputs is not None:
            if custom_outputs:
                template["outputs"] = custom_outputs
        else:
            fields = _output_fields_for_type(self.return_type)
            if fields == ["out"]:
                template["outputs"] = {
                    "parameters": [
                        {"name": "out", "valueFrom": {"path": "/tmp/dewret-out"}}
                    ]
                }
            else:
                template["outputs"] = {
                    "parameters": [
                        {"name": f, "valueFrom": {"path": f"/tmp/dewret-out/{f}"}}
                        for f in fields
                    ]
                }

        return template

    def render_dag_task(self) -> dict[str, RawType]:
        """Render the DAG task entry for this step.

        Returns: an Argo DAG task dict.
        """
        task: dict[str, RawType] = {
            "name": self.name,
            "template": self.name,
        }

        if self.dependencies:
            task["dependencies"] = self.dependencies

        if self.arguments:
            task["arguments"] = {
                "parameters": [
                    {
                        "name": key,
                        "value": (
                            ref.value
                            if isinstance(ref, ReferenceDefinition)
                            else str(firm_to_raw(ref.value))
                        ),
                    }
                    for key, ref in self.arguments.items()
                ]
            }

        return task


@define
class WorkflowDefinition:
    """Argo-renderable workflow.

    Coerces the dewret structure of a workflow into a native Argo
    Workflows resource.

    Attributes:
        steps: sequence of step definitions.
        parameters: workflow-level input parameters.
        name: workflow name.
        kind: "Workflow" or "WorkflowTemplate".
        namespace: optional K8s namespace.
        generate_name: optional generateName (mutually exclusive with name).
        config: full renderer configuration.
    """

    steps: list[StepDefinition]
    parameters: list[dict[str, RawType]]
    name: str | None
    kind: str
    namespace: str | None
    generate_name: str | None
    config: ArgoRendererConfiguration

    @classmethod
    def from_workflow(
        cls, workflow: Workflow, name: str | None = None, **config_kwargs: Any
    ) -> "WorkflowDefinition":
        """Build from a Workflow.

        Args:
            workflow: workflow to convert.
            name: name for the workflow, or None.
            **config_kwargs: overrides merged into the active configuration.
        """
        from dewret.core import get_render_configuration

        config = dict(default_config())
        # Pull active render config
        for key in ArgoRendererConfiguration.__annotations__:
            val = get_render_configuration(key)
            if val is not None:
                config[key] = val
        config.update(config_kwargs)

        kind = config.get("kind", "Workflow")
        namespace = config.get("namespace")
        generate_name = config.get("generate_name")
        config_name = config.get("name")

        # Build steps (skip factories if needed)
        steps = [
            StepDefinition.from_step(step)
            for step in workflow.indexed_steps.values()
            if not isinstance(step, FactoryCall)
        ]

        # Extract workflow-level parameters
        params: list[dict[str, RawType]] = []
        for p in sorted(workflow.find_parameters(), key=lambda p: p.name):
            param_def: dict[str, RawType] = {"name": _k8s_name(p.name)}
            default = flatten_if_set(p.__default__)
            if not isinstance(default, Unset):
                param_def["value"] = str(firm_to_raw(default))
            params.append(param_def)

        # Determine workflow name: explicit name > config name > generate_name > workflow._name > fallback
        wf_name = name or config_name or (
            None
            if generate_name
            else _k8s_name(
                getattr(workflow, "_name", None) or "dewret-workflow"
            )
        )

        return cls(
            steps=steps,
            parameters=params,
            name=wf_name,
            kind=kind,
            namespace=namespace,
            generate_name=generate_name,
            config=config,
        )

    def _render_dag_template(self) -> dict[str, RawType]:
        """Render the main DAG entrypoint template.

        Returns: a template dict with a 'dag' key containing all tasks.
        """
        dag_tasks = [step.render_dag_task() for step in self.steps]
        return {
            "name": "main",
            "dag": {"tasks": dag_tasks},
        }

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns: a complete Argo Workflow/WorkflowTemplate resource dict.
        """
        metadata: dict[str, RawType] = {}
        if self.generate_name:
            metadata["generateName"] = self.generate_name
        elif self.name:
            metadata["name"] = self.name
        if self.namespace:
            metadata["namespace"] = self.namespace

        spec: dict[str, RawType] = {
            "entrypoint": "main",
            "templates": [
                self._render_dag_template(),
                *[step.render_template(self.config) for step in self.steps],
            ],
        }

        if self.parameters:
            spec["arguments"] = {"parameters": self.parameters}

        volumes = self.config.get("volumes")
        if volumes:
            spec["volumes"] = volumes

        return {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": self.kind,
            "metadata": metadata,
            "spec": spec,
        }


def render(
    workflow: Workflow, **kwargs: Unpack[ArgoRendererConfiguration]
) -> dict[str, dict[str, RawType]]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.
        **kwargs: additional configuration arguments matching ArgoRendererConfiguration.

    Returns: a dict keyed by workflow name, with ``__root__`` for the primary workflow.
    """
    with set_render_configuration(kwargs):  # type: ignore
        rendered = base_render(
            workflow,
            lambda w: WorkflowDefinition.from_workflow(w).render(),
        )
    return rendered
