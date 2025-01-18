# Copyright 2024- Flax & Teal Limited. All Rights Reserved.
# Copyright 2022 The Kubeflow Authors [portions from Kubeflow Pipelines]
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

import uuid
import itertools
import warnings
from google.protobuf import json_format
from kfp.pipeline_spec import pipeline_spec_pb2
from kfp.compiler import pipeline_spec_builder as builder
from kfp import dsl
from kfp.dsl.types import type_utils
from kfp.dsl.pipeline_context import Pipeline
from attrs import define, has as attrs_has, fields as attrs_fields, AttrsInstance
from dataclasses import is_dataclass, fields as dataclass_fields
from collections.abc import Mapping
import yaml
from typing import (
    TypeVar,
    Annotated,
    NamedTuple,
    TypedDict,
    NotRequired,
    get_origin,
    get_args,
    cast,
    Any,
    Unpack,
    Iterable,
    Callable,
    Optional,
    List,
)
from types import UnionType
import inspect
from inspect import isclass, getsourcefile, getsource
from pathlib import Path
from sympy import Basic, Tuple, Dict, jscode, Symbol
from contextvars import ContextVar

from dewret.data import Dataset, DatasetPath
from dewret.core import (
    Raw,
    RawType,
    FirmType,
)
from dewret.workflow import (
    FactoryCall,
    Workflow,
    BaseStep,
    NestedStep,
    StepReference,
    ParameterReference,
    DatasetParameterReference,
    DatasetParameter,
    expr_to_references,
)
from dewret.utils import (
    crawl_raw,
    DataclassProtocol,
    firm_to_raw,
    flatten_if_set,
    Unset,
)
from dewret.render import base_render
from dewret.core import Reference, get_render_configuration, set_render_configuration, strip_annotations

T = TypeVar("T")
PIPELINE: ContextVar[Pipeline] = ContextVar("pipeline")
CHANNELS: ContextVar[dict[Reference[Any], dsl.pipeline_channel.PipelineChannel]] = (
    ContextVar("channels")
)
KFPDataset = Annotated[T, "KFPDataset"]

def extend_signature(func, inputs, return_ann):
    import inspect
    from collections import OrderedDict
    sig = inspect.signature(func)
    parameters = OrderedDict()
    for missing_input in inputs - set(sig.parameters):
        parameters[missing_input] = inspect.Parameter(missing_input, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=dsl.Input[dsl.Artifact]) # Check
    parameters["Output"] = inspect.Parameter(return_ann, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=dsl.Output[dsl.Artifact])
    return parameters

# pipelines/sdk/python/kfp/dsl/component_factory.py
def create_component_from_step(
    step: BaseStep,
    component_spec: dsl.structures.ComponentSpec,
    base_image: Optional[str] = None,
    target_image: Optional[str] = None,
    packages_to_install: List[str] = None,
    pip_index_urls: Optional[List[str]] = None,
    output_component_file: Optional[str] = None,
    install_kfp_package: bool = True,
    kfp_package_path: Optional[str] = None,
    pip_trusted_hosts: Optional[List[str]] = None,
    use_venv: bool = False,
) -> dsl.python_component.PythonComponent:
    """Implementation for the @component decorator.

    The decorator is defined under component_decorator.py. See the
    decorator for the canonical documentation for this function.
    """

    packages_to_install_command = dsl.component_factory._get_packages_to_install_command(
        install_kfp_package=install_kfp_package,
        target_image=target_image,
        kfp_package_path=kfp_package_path,
        packages_to_install=packages_to_install,
        pip_index_urls=pip_index_urls,
        pip_trusted_hosts=pip_trusted_hosts,
        use_venv=use_venv,
    )

    command = []
    args = []
    if base_image is None:
        base_image = dsl.component_factory._DEFAULT_BASE_IMAGE
        warnings.warn(
            ("The default base_image used by the @dsl.component decorator will switch from 'python:3.9' to 'python:3.10' on Oct 1, 2025. To ensure your existing components work with versions of the KFP SDK released after that date, you should provide an explicit base_image argument and ensure your component works as intended on Python 3.10."
            ),
            FutureWarning,
            stacklevel=2,
        )

    component_image = base_image
    func = step.task.target

    if target_image:
        component_image = target_image
        command, args = dsl.component_factory._get_command_and_args_for_containerized_component(
            function_name=func.__name__,)
    else:
        command, args = dsl.component_factory._get_command_and_args_for_lightweight_component(
            func=func)
        # RMV - globals!?
        # Need to strip signature of annotations for original function as not guaranteed to be imported.
        sig = inspect.signature(func)
        return_tuple = False
        output_name = py_name(step.name)
        def to_repr(typ):
            nonlocal return_tuple
            return_ann, artifacts = to_kfp_type(output_name, typ)
            if artifacts:
                return_ann = artifacts[output_name].__qualname__
            else:
                return_type = return_ann["type"]
                if hasattr(return_type, "_fields"):
                    annotations = [(key, to_repr(return_type.__annotations__[key])) for key in return_type._fields]
                    return_tuple = annotations # what if nested?
                    annotations = ", ".join(f"('{k}', {v})" for k, v in annotations)
                    command[-1] += f"{return_type.__name__} = NamedTuple('{return_type.__name__}', ({annotations}))\n"
                return_ann = return_type.__name__
            return return_ann
        return_ann = to_repr(step.return_type)
        signature = []
        in_paths = []
        for param in sig.parameters:
            ann, artifacts = to_kfp_type(param, sig.parameters[param].annotation)
            if artifacts:
                signature.append((param, f"Input[{artifacts[param].__qualname__}]"))
                in_paths.append(param)
            else:
                signature.append((param, f"{sig.parameters[param].annotation.__qualname__}"))
        output_datasets = {}
        wrapper_str = ', '.join(f'{n}: {t}' for n, t in signature)
        print(step.return_type)
        command[-1] += """
from kfp.dsl.types.artifact_types import *
import typing
from typing import NamedTuple
import os
import shutil
from tempfile import mkstemp
from pathlib import Path
"""
        dataset_parameters = []
        if return_tuple:
            output_param = ", ".join(f"{key}: dsl.Output[{ann}]" for key, ann in return_tuple)
        else:
            output_param = f"{output_name}: dsl.Output[{return_ann}]"
        command[-1] += f"def {func.__name__}_({wrapper_str}, {output_param}):\n    paths = {{}}\n    unpaths = {{}}\n"
        for p in in_paths:
            command[-1] += f"    {p} = {p}.path\n"
        dataset_parameters = []
        for key, arg in step.arguments.items():
            if isinstance(arg, DatasetParameterReference):
                command[-1] += f"    f, {key} = mkstemp(); os.close(f)\n"
                command[-1] += f"    paths['{key}'] = Path({key})\n"
                command[-1] += f"    unpaths[Path({key})] = 0\n"
                dataset_parameters.append((key, arg))
        command[-1] += f"    globals().update(paths)\n    final_output = {func.__name__}({', '.join(f'{a}={a}' for a in sig.parameters)})\n"
        if return_tuple:
            command[-1] += f"    {output_name} = ({', '.join(key for key, _ in return_tuple)})\n"
            command[-1] += f"    for p, q in zip(final_output, {output_name}): shutil.move(p, q.path)\n"
        else:
            command[-1] += f"    shutil.move(final_output, {output_name}.path)\n"
        command[-1] += "    for p in unpaths: shutil.rmtree(str(p), ignore_errors=True)\n"
        # we could use unpaths[final_output] to update metadata here.
        args[-1] += "_"

    component_spec.implementation = dsl.structures.Implementation(
        container=dsl.structures.ContainerSpecImplementation(
            image=component_image,
            command=packages_to_install_command + command,
            args=args,
        ))

    module_path = Path(getsourcefile(func))
    module_path.resolve()

    component_name = dsl.component_factory._python_function_name_to_component_name(func.__name__)
    component_info = dsl.component_factory.ComponentInfo(
        name=component_name,
        function_name=func.__name__,
        func=func,
        target_image=target_image,
        module_path=module_path,
        component_spec=component_spec,
        output_component_file=output_component_file,
        base_image=base_image,
        packages_to_install=packages_to_install,
        pip_index_urls=pip_index_urls,
        pip_trusted_hosts=pip_trusted_hosts)

    if dsl.component_factory.REGISTERED_MODULES is not None:
        dsl.component_factory.REGISTERED_MODULES[component_name] = component_info

    if output_component_file:
        component_spec.save_to_component_yaml(output_component_file)

    return dsl.python_component.PythonComponent(
        component_spec=component_spec, python_func=func)

def get_name_to_specs(func_params, return_ann, step_name: str, containerized: bool = False):
    name_to_input_specs = {}
    name_to_output_specs = {}
    # in_artifacts = {}

    for key, func_param in func_params:
        func_param, ann = strip_annotations(func_param)
        typ, _ = to_kfp_type(key, func_param)
        if dsl.types.type_annotations.OutputAnnotation in ann:
            # Trying to remove this on the basis that having a single output,
            # while a strong constraint, is not a hard limitation (tuples are possible)
            # and it lets us create an idiomatic graph.
            ...
            # name_to_output_specs[key] = dsl.structures.OutputSpec(
            #     **typ,
            # )
        else:
            name_to_input_specs[key] = dsl.structures.InputSpec(
                **typ,
            )
        # if set(in_artifacts) & set(input_artifacts):
        #     raise TypeError(f"Clashing naming keys for input artifacts: {in_artifacts} -- {input_artifacts}")
        # in_artifacts.update(input_artifacts)
    # if containerized:
    #     if return_ann not in [
    #             inspect.Parameter.empty,
    #             structures.ContainerSpec,
    #     ]:
    #         raise TypeError(
    #             'Return annotation should be either ContainerSpec or omitted for container components.'
    #         )
    # ignore omitted returns
    if return_ann is None:
        pass
    prefix = py_name(step_name) or dsl.component_factory.SINGLE_OUTPUT_NAME
    return_type, _ = to_kfp_type(prefix, return_ann)
    return_type = return_type["type"]
    # is NamedTuple
    if hasattr(return_type, "_fields"):
        output_specs, _ = make_output_spec(prefix, return_ann)
        # if set(out_artifacts) & set(return_artifacts):
        #     raise TypeError(f"Clashing artifact names: {out_artifacts} -- {return_artifacts}")
        # name_to_output_specs.update(return_artifacts)
        for name, output_spec in output_specs.items():
            if output_spec is not None:
                name_to_output_specs[name] = output_spec
    else:
        rettyp, _ = make_output_spec(
            dsl.component_factory.SINGLE_OUTPUT_NAME, return_ann
        )
        # name_to_output_specs.update(return_artifacts)
        if rettyp is not None:
            name_to_output_specs[prefix] = rettyp
    # if set(name_to_input_specs) & set(in_artifacts):
    #     raise TypeError(f"Clashing artifact names with parameters: {in_artifacts} -- {name_to_input_specs}")
    # name_to_input_specs.update({
    #     key: dsl.structures.InputSpec(
    #         **dsl.component_factory.make_input_output_spec_args(art)
    #     )
    #     for key, art in in_artifacts.items()
    # })
    return name_to_input_specs, name_to_output_specs

def ensure_channels(expression: Any, task_name: str | None) -> Any:
    def remap(ref):
        if isinstance(ref, Reference) and not isinstance(ref, DatasetParameterReference):
            # RMV: is this OK re. artifacts?
            if ref not in channels:
                kfp_type, artifacts = to_kfp_type(ref.name, with_type(ref))
                # if kfp_type["type"] != "Artifact":
                channels[ref] = dsl.pipeline_channel.create_pipeline_channel(
                    name=py_name(ref.name),
                    channel_type=kfp_type["type"],  # type: ignore
                    task_name=k8s_name(ref._.step.name),
                    is_artifact_list=False,
                )
                # for key, art in artifacts.items():
                #     if key not in channels:
                #         spec_args = dsl.component_factory.make_input_output_spec_args(art)
                #         channels[key] = dsl.pipeline_channel.create_pipeline_channel(
                #             name=k8s_name(key),
                #             channel_type=spec_args["type"],  # type: ignore
                #             task_name=k8s_name(key),
                #             is_artifact_list=spec_args["is_artifact_list"],
                #         )
            return channels[ref]
        elif isinstance(ref, Raw):
            return ref.value

    channels = CHANNELS.get()
    expr, to_check = expr_to_references(expression, remap=remap)
    return expr


class DewretPipelineTask(dsl.pipeline_task.PipelineTask):
    def __init__(
        self,
        component_spec: dsl.structures.ComponentSpec,
        args: dict[str, Any],
        execute_locally: bool = False,
        execution_caching_default: bool = True,
        output: StepReference[Any] | None = None,
    ) -> None:
        """Initilizes a PipelineTask instance."""
        # import within __init__ to avoid circular import
        from kfp.dsl.tasks_group import TasksGroup

        self.state = dsl.pipeline_task.TaskState.FUTURE
        self.parent_task_group: None | TasksGroup = None
        args = args or {}

        if component_spec.inputs:
            for input_name, argument_value in args.items():
                if input_name not in component_spec.inputs:
                    raise ValueError(
                        f"Component {component_spec.name!r} got an unexpected input:"
                        f" {input_name!r}."
                    )

                input_spec = component_spec.inputs[input_name]

                # TODO: we cannot use this as-is, since the value->type
                # map is not the same as dewret.
                # type_utils.verify_type_compatibility(
                #     given_value=argument_value,
                #     expected_spec=input_spec,
                #     error_message_prefix=(
                #         f"Incompatible argument passed to the input "
                #         f"{input_name!r} of component {component_spec.name!r}: "
                #     ),
                # )

        self.component_spec = component_spec

        self._task_spec = dsl.structures.TaskSpec(
            name=self._register_task_handler(),
            inputs=dict(args.items()),
            dependent_tasks=[],
            component_ref=component_spec.name,
            enable_caching=execution_caching_default,
        )
        self._run_after: list[str] = []

        self.importer_spec = None
        self.container_spec = None
        self.pipeline_spec = None
        self._ignore_upstream_failure_tag = False
        # platform_config for this primitive task; empty if task is for a graph component
        self.platform_config = {}

        def validate_placeholder_types(
            component_spec: dsl.structures.ComponentSpec,
        ) -> None:
            inputs_dict = component_spec.inputs or {}
            outputs_dict = component_spec.outputs or {}
            for arg in itertools.chain(
                (component_spec.implementation.container.command or []),
                (component_spec.implementation.container.args or []),
            ):
                dsl.pipeline_task.check_primitive_placeholder_is_used_for_correct_io_type(
                    inputs_dict, outputs_dict, arg
                )

        if component_spec.implementation.container is not None:
            validate_placeholder_types(component_spec)
            self.container_spec = self._extract_container_spec_and_convert_placeholders(
                component_spec=component_spec
            )
        elif component_spec.implementation.importer is not None:
            self.importer_spec = component_spec.implementation.importer
            self.importer_spec.artifact_uri = args["uri"]
        else:
            self.pipeline_spec = self.component_spec.implementation.graph

        self._outputs = {output.name: ensure_channels(output, component_spec.name)}

        # args = {arg: ensure_channels(arg) for arg in args}
        self._inputs = args

        self._channel_inputs = [
            value
            for _, value in args.items()
            if isinstance(value, dsl.pipeline_channel.PipelineChannel)
        ] + dsl.pipeline_channel.extract_pipeline_channels_from_any(
            [
                value
                for _, value in args.items()
                if not isinstance(value, dsl.pipeline_channel.PipelineChannel)
            ]
        )

        if execute_locally:
            self._execute_locally(args=args)


def register_task_handler(
    task: dsl.pipeline_task.PipelineTask,
) -> dsl.pipeline_task.PipelineTask:
    """Registers task handler for attaching tasks to pipelines.

    Args:
        task: task to add to pipeline.
    """
    pipeline = PIPELINE.get()
    name = pipeline.add_task(
        task=task, add_to_group=not getattr(task, "is_exit_handler", False)
    )
    return name


dsl.pipeline_task.PipelineTask._register_task_handler = register_task_handler


class BuilderPipeline(Pipeline):
    """ContextVar-based Pipeline."""

    old_pipeline: Pipeline | None = None

    def __enter__(self) -> "BuilderPipeline":
        """Ensure a pipeline is set for tasks created in this context."""
        # if Pipeline._default_pipeline:
        #     raise Exception("Nested pipelines are not allowed.")

        Pipeline._default_pipeline = self

        try:
            self.old_pipeline = PIPELINE.get()
        except LookupError:
            ...
        PIPELINE.set(self)
        CHANNELS.set({})

        return self

    def __exit__(self, *_: Any) -> None:
        """Reset the pipeline for new tasks to None."""
        PIPELINE.set(self.old_pipeline)
        CHANNELS.set({})
        Pipeline._default_pipeline = None


class CommandInputSchema(TypedDict):
    """Structure for referring to a raw type in CWL.

    Encompasses several CWL types. In future, it may be best to
    use _cwltool_ or another library for these basic structures.

    Attributes:
        type: CWL type of this input.
        label: name to show for this input.
        fields: (for `record`) individual fields in a dict-like structure.
        items: (for `array`) type that each field will have.
    """

    type: "InputSchemaType"
    label: str
    fields: NotRequired[dict[str, "CommandInputSchema"]]
    items: NotRequired["InputSchemaType"]
    default: NotRequired[RawType]
    artifacts: NotRequired[list[type[DatasetPath]]]


InputSchemaType = (
    str
    | CommandInputSchema
    | list[str]
    | list["InputSchemaType"]
    | dict[str, "str | InputSchemaType"]
    | DatasetPath
)


def render_expression(ref: Any) -> "ReferenceDefinition":
    """Turn a rich (sympy) expression into a CWL JS expression.

    Args:
        ref: a structure whose elements are all string-renderable or sympy Basic.

    Returns: a ReferenceDefinition containing a string representation of the expression in the form `$(...)`.
    """

    def _render(ref: Any) -> Basic | RawType:
        if not isinstance(ref, Basic):
            if isinstance(ref, Mapping):
                ref = Dict({key: _render(val) for key, val in ref.items()})
            elif not isinstance(ref, str | bytes) and isinstance(ref, Iterable):
                ref = Tuple(*(_render(val) for val in ref))
        return ref

    expr = _render(ref)
    if isinstance(expr, Basic):
        values = list(expr.free_symbols)
        step_syms = [sym for sym in expr.free_symbols if isinstance(sym, StepReference)]
        param_syms = [
            sym for sym in expr.free_symbols if isinstance(sym, ParameterReference)
        ]

        if set(values) != set(step_syms) | set(param_syms):
            raise NotImplementedError(
                f"Can only build expressions for step results and param results: {ref}"
            )

        if len(step_syms) > 1:
            raise NotImplementedError(
                f"Can only create expressions with 1 step reference: {ref}"
            )
        if not (step_syms or param_syms):
            ...
        if values == [ref]:
            if isinstance(ref, StepReference):
                return ReferenceDefinition(source=to_name(ref), value_from=None)
            else:
                return ReferenceDefinition(source=ref.name, value_from=None)
        source = None
        for ref in values:
            if isinstance(ref, StepReference):
                field = with_field(ref)
                parts = field.split("/")
                base = f"/{parts[0]}" if parts and parts[0] else ""
                if len(parts) > 1:
                    expr = expr.subs(ref, f"self.{'.'.join(parts[1:])}")
                else:
                    expr = expr.subs(ref, "self")
                source = f"{ref.__root_name__}{base}"
            else:
                expr = expr.subs(ref, Symbol(f"inputs.{ref.name}"))
        return ReferenceDefinition(
            source=source, value_from=f"$({jscode(_render(expr))})"
        )
    return ReferenceDefinition(source=str(expr), value_from=None)


class ExecutorConfiguration(TypedDict):
    packages: list[str]

class KubeflowRendererConfiguration(TypedDict):
    """Configuration for the renderer.

    Attributes:
        executor: settings to pass on to the executor.
    """

    executor: NotRequired[dict[str, ExecutorConfiguration]]


def default_config() -> KubeflowRendererConfiguration:
    """Default configuration for this renderer.

    This is a hook-like call to give a configuration dict that this renderer
    will respect, and sets any necessary default values.

    Returns: a dict with (preferably) raw type structures to enable easy setting
        from YAML/JSON.
    """
    return {
        "executor": {"default": {"packages": []}},
    }


def with_type(result: Any) -> type | Any:
    """Get a Python type from a value.

    Does so either by using its `__type__` field (for example, for References)
    or if unavailable, using `type()`.

    Returns: a Python type.
    """
    if hasattr(result, "__type__"):
        return result.__type__
    return type(result)


def with_field(result: Any) -> str:
    """Get a string representing any 'field' suffix of a value.

    This only makes sense in the context of a Reference, which can represent
    a deep reference with a known variable (parameter or step result, say) using
    its `__field__` attribute. Defaults to `"out"` as this produces compliant CWL
    where every output has a "fieldname".

    Returns: a string representation of the field portion of the passed value or `"out"`.
    """
    if hasattr(result, "__field__") and result.__field__:
        return str(result.__field_str__)
    else:
        return "out"


def to_name(result: Reference[Any]) -> str:
    """Take a reference and get a name representing it.

    The primary purpose of this method is to deal with the case where a reference is to the
    whole result, as we always put this into an imagined `out` field for CWL consistency.

    Returns: the name of the reference, including any field portion, appending an `"out"` fieldname if none.
    """
    if (
        hasattr(result, "__field__")
        and not result.__field__
        and isinstance(result, StepReference)
    ):
        return f"{result.__name__}/out"
    return result.__name__


@define
class ReferenceDefinition:
    """CWL-renderable internal reference.

    Normally points to a value or a step.
    """

    source: str | None
    value_from: str | None

    @classmethod
    def from_reference(cls, ref: Reference[Any]) -> "ReferenceDefinition":
        """Build from a `Reference`.

        Converts a `dewret.workflow.Reference` into a CWL-rendering object.

        Args:
            ref: reference to convert.
        """
        return render_expression(ref)

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        representation: dict[str, RawType] = {}
        if self.source is not None:
            representation["source"] = self.source
        if self.value_from is not None:
            representation["valueFrom"] = self.value_from
        return representation


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
    out: dict[str, "CommandInputSchema"] | list[str]
    in_: Mapping[str, ReferenceDefinition | Raw]

    @classmethod
    def from_step(cls, step: BaseStep) -> "StepDefinition":
        """Build from a `BaseStep`.

        Converts a `dewret.workflow.Step` into a CWL-rendering object.

        Args:
            step: step to convert.
        """
        # Uses of global dataset parameters are to create datasets for output,
        # equivalent to KFP's Output[Artifact] annotation.
        # Ignore dataset parameter references when constructing the function.
        # They will be _actual_ globals when it runs.
        param_types = [(key, with_type(value)) for key, value in step.arguments.items() if not isinstance(value, DatasetParameterReference)]
        inputs, outputs = get_name_to_specs(param_types, step.return_type, step_name=k8s_name(step.name))
        executor_config = get_render_configuration("executor")["default"]

        default_image = executor_config.get("image", "python:3.9")
        default_packages = executor_config.get("packages")
        default_pip_index_urls = executor_config.get("pip_index_urls")
        default_kfp_package_path = executor_config.get("kfp_package_path")
        default_pip_trusted_hosts = executor_config.get("pip_trusted_hosts")
        container = dsl.structures.ContainerSpecImplementation(
            image="python:3.9",
            command=["python"], # RMV
            args=[],
        )
        component_spec = dsl.structures.ComponentSpec(
            name=step.name,
            description=f"{step.name} via dewret",
            inputs=inputs,
            # outputs=make_output_spec("out", step.return_type)["fields"], # make_output_spec(return_ann)
            outputs=outputs,  # make_output_spec(return_ann)
            implementation=dsl.structures.Implementation(container)
        )

        if isinstance(step, NestedStep):
            cmpt = dsl.container_component_class.ContainerComponent(component_spec, step.task)
        else:
            def fn(*args, **kwargs):
                ...
            cmpt = create_component_from_step(
                base_image=default_image,
                component_spec=component_spec,
                packages_to_install=default_packages,
                pip_index_urls=default_pip_index_urls,
                kfp_package_path=default_kfp_package_path,
                pip_trusted_hosts=default_pip_trusted_hosts,
                step=step
            )

        task_inputs = {
            key: ensure_channels(
                arg,
                step.name
            ) for key, arg in step.arguments.items()
            if not isinstance(arg, DatasetParameterReference)
        }
        task_spec = DewretPipelineTask(
            cmpt.component_spec,
            task_inputs,
            output=step.make_reference(workflow=step.__workflow__),
        )
        component_spec.implementation = dsl.structures.Implementation(
            container=dsl.structures.ContainerSpecImplementation(
                image="IMAGE",
                command="python",
                args=[
                    "--executor_input",
                    dsl.PIPELINE_TASK_EXECUTOR_INPUT_PLACEHOLDER,
                    "--function_to_execute",
                    step.task.name,
                ],
            )
        )
        return task_spec

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
                    if isinstance(ref, ReferenceDefinition)
                    else render_expression(ref).render()
                    if isinstance(ref, Basic)
                    else {"default": firm_to_raw(ref.value)}
                    if hasattr(ref, "value")
                    else render_expression(ref).render()
                )
                for key, ref in self.in_.items()
            },
            "out": crawl_raw(self.out),
        }


def dataset_path_to_artifact(typ):
    typ, annotateds = strip_annotations(typ)
    if "KFPDataset" in annotateds:
        typ = dsl.types.artifact_types.Dataset
    else:
        typ = dsl.types.artifact_types.Artifact
    return typ

def to_kfp_type(label: str, full_typ: type) -> tuple[CommandInputSchema, dict[str, type[dsl.types.artifact_types.Artifact]]]:
    """Map Python types to CWL types.

    Args:
        label: the label for the variable being checked to prefill the input def and improve debugging info.
        typ: a Python basic type.

    Returns:
        CWL specification type dict.
    """
    typ, annotateds = strip_annotations(full_typ)
    typ_dict: CommandInputSchema = {"type": ""}
    base: Any | None = typ
    artifacts = {}
    args = get_args(typ)
    if args:
        base = get_origin(typ)

    if base == type(None):
        typ_dict["type"] = "null"
    elif base == int:
        typ_dict["type"] = "Integer"
    elif base == bool:
        typ_dict["type"] = "Boolean"
    elif base == dict or (isinstance(base, type) and attrs_has(base)):
        typ_dict["type"] = "Dict"
    elif base == float:
        typ_dict["type"] = "Float"
    elif base == str:
        typ_dict["type"] = "String"
    elif base == bytes:
        raise RuntimeError("KFP cannot currently handle bytes as a annotation type.")
    elif isinstance(typ, UnionType):
        raise RuntimeError("KFP cannot currently handle unions as a annotation type.")
        #typ_dict.update(
        #    {"type": NamedTuple(label, ((f"item{n}", item) for n, item in enumerate(args)))}
        #)
        #typ_dict["type"].__annotations__ = {f"item{n}": item for n, item in enumerate(args)}
    elif isclass(base) and issubclass(base, Iterable):
        try:
            if len(args) > 1:
                # This is only true for a pipeline - components can output only one artifact.
                # artifact_args = [arg for arg in args if issubclass(strip_annotateds(arg)[0], DatasetPath)]
                # if artifact_args:
                #     if len(args) != len(artifact_args):
                #         raise TypeError(f"Tuple return must be all artifacts or no artifacts: {args} -- {artifact_args}")
                #     if len({type(arg) for arg in args}) != 1:
                #         raise TypeError(f"Can only have one artifact type in a tuple: {arg}")
                #     print(artifact_args, label)
                #     typ_dict.update(dsl.component_factory.make_input_output_spec_args(list[dataset_path_to_artifact(artifact_args[0])]))
                # else:
                tuple_label = label.replace("-", "_")
                typ_dict.update(
                    {
                        "type": NamedTuple(tuple_label, ((f"{tuple_label}__{n}", item) for n, item in enumerate(args)))
                    }
                )
                typ_dict["type"].__annotations__ = {f"{tuple_label}__{n}": item for n, item in enumerate(args)}
            elif len(args) == 1:
                interior_typ, interior_artifacts = to_kfp_type(label, args[0])
                typ_dict.update(
                    {"type": f"List[{interior_typ["type"]}"}
                )
                if set(artifacts.keys()) & set(interior_artifacts.keys()):
                    raise TypeError(f"Artifacts have overlapping keys: {artifacts} -- {interior_artifacts}")
                artifacts.update(interior_artifacts)
            else:
                typ_dict["type"] = "array"
        except IndexError as err:
            raise TypeError(
                f"Cannot render complex type ({typ}) to CWL for {label}, have you enabled allow_complex_types configuration?"
            ) from err
    elif get_render_configuration("allow_complex_types"):
        typ_dict["type"] = typ if isinstance(typ, str) else typ.__name__
    elif isinstance(typ, type) and issubclass(typ, Dataset):
        artifacts[label] = dataset_path_to_artifact(full_typ)
        typ_dict.update(dsl.component_factory.make_input_output_spec_args(artifacts[label]))
    elif typ:
        raise TypeError(f"Cannot render type ({typ}) to CWL for {label}; base: {base}; args: {args}")
    return typ_dict, artifacts


class CommandOutputSchema(CommandInputSchema):
    """Structure for referring to an output in CWL.

    As a simplification, this is an input schema with an extra
    `outputSource` field.

    Attributes:
        outputSource: step result to use for this output.
    """

    outputSource: NotRequired[str]
    expression: NotRequired[str]
    source: NotRequired[list[str]]


def make_output_spec(
    label: str,
    typ: type[RawType | AttrsInstance | DataclassProtocol],
    output_source: str | None = None,
) -> tuple[dsl.structures.OutputSpec, dict[str, type[DatasetPath]]]:
    """Turn a step's output into an output schema.

    Takes a source, type and label and provides a description for CWL.

    Args:
        label: name of this field.
        typ: either a basic type, compound of basic types, or a TypedDict representing a pre-defined result structure.
        output_source: if provided, a CWL step result reference to input here.

    Returns:
        CWL CommandOutputSchema-like structure for embedding into an `outputs` block
    """
    fields = None
    artifacts = {}
    if attrs_has(typ):
        fields = {}
        for field in attrs_fields(typ):
            output_spec, field_artifacts = make_output_spec(field.name, field.type)
            fields[str(field.name)] = cast(
                dsl.structures.OutputSpec, output_spec
            )
            if set(artifacts) & set(field_artifacts):
                raise TypeError(f"Clashing key names: {artifacts} -- {field_artifacts}")
            artifacts.update(field_artifacts)
    elif is_dataclass(typ):
        fields = {}
        for field in dataclass_fields(typ):
            output_spec, field_artifacts = make_output_spec(field.name, field.type)
            fields[str(field.name)] = cast(
                dsl.structures.OutputSpec, output_spec
            )
            if set(artifacts) & set(field_artifacts):
                raise TypeError(f"Clashing key names: {artifacts} -- {field_artifacts}")
            artifacts.update(field_artifacts)
    else:
        kfp_type, _ = to_kfp_type(label, typ)
        kfp_type = kfp_type["type"]
        if hasattr(kfp_type, "_fields"):
            fields = {}
            for name in kfp_type._fields:
                output_spec, field_artifacts = make_output_spec(name, kfp_type.__annotations__[name])
                fields[name] = cast(
                    dsl.structures.OutputSpec, output_spec
                )
                if set(artifacts) & set(field_artifacts):
                    raise TypeError(f"Clashing key names: {artifacts} -- {field_artifacts}")
                artifacts.update(field_artifacts)

    if fields:
        output = fields
    else:
        # TODO: this complains because NotRequired keys are never present,
        # but that does not seem like a problem here - likely a better solution.
        kfp_type, inner_artifacts = to_kfp_type(label, typ)
        if set(artifacts) & set(inner_artifacts):
            raise TypeError(f"Clashing key names: {artifacts} -- {inner_artifacts}")
        artifacts.update({
            key: dsl.structures.OutputSpec(
                **dsl.component_factory.make_input_output_spec_args(art)
            ) for key, art in inner_artifacts.items()
        })
        output = dsl.structures.OutputSpec(**kfp_type)
    # if output_source is not None:
    #     output["outputSource"] = output_source
    return output, artifacts


@define
class OutputsDefinition:
    """CWL-renderable set of workflow outputs.

    Turns dewret results into a CWL output block.

    Attributes:
        outputs: sequence of results from a workflow.
    """

    outputs: (
        dict[str, "CommandOutputSchema"]
        | list["CommandOutputSchema"]
        | CommandOutputSchema
    )

    @classmethod
    def from_results(
        cls,
        results: dict[str, StepReference[Any]]
        | list[StepReference[Any]]
        | tuple[StepReference[Any], ...],
    ) -> "OutputsDefinition":
        """Takes a mapping of results into a CWL structure.

        Pulls the result type from the signature, ultimately, if possible.

        Returns:
            CWL-like structure representing all workflow outputs.
        """

        def _build_results(result: Any) -> RawType:
            if isinstance(result, Reference):
                # TODO: need to work out how to tell mypy that a TypedDict is also dict[str, RawType]
                return make_output_spec(  # type: ignore
                    with_field(result), with_type(result), output_source=to_name(result)
                )
            results = result
            return (
                [_build_results(result) for result in results]
                if isinstance(results, list | tuple | Tuple)
                else {key: _build_results(result) for key, result in results.items()}
            )

        try:
            # TODO: sort out this nested type building.
            return cls(outputs=_build_results(results))  # type: ignore
        except AttributeError:
            expr, references = expr_to_references(results)
            reference_names = sorted(
                {
                    str(ref._.parameter)
                    if isinstance(ref, ParameterReference)
                    else str(ref._.step)
                    for ref in references
                }
            )
            return cls(
                outputs={
                    "out": {
                        "type": "float",  # WARNING: we assume any arithmetic expression returns a float.
                        "label": "out",
                        "expression": str(expr),
                        "source": reference_names,
                    }
                }
            )

    def render(self) -> dict[str, RawType] | list[RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        return (
            [crawl_raw(output) for output in self.outputs]
            if isinstance(self.outputs, list)
            else {key: crawl_raw(output) for key, output in self.outputs.items()}
        )


def py_name(name: str | None) -> str | None:
    return name and name.replace("-", "_").replace("[", "__").replace("]", "")

def k8s_name(name: str | None) -> str | None:
    return name and name.replace("_", "-").replace("[", "--").replace("]", "")

class DewretGraphComponent(dsl.base_component.BaseComponent):
    """CWL-renderable workflow.

    Coerces the dewret structure of a workflow into that
    needed for valid CWL.

    Attributes:
        steps: sequence of steps in the workflow.
    """

    @classmethod
    def from_workflow(
        cls, workflow: Workflow, name: None | str = None, execute: bool = True
    ) -> "DewretGraphComponent":
        """Build from a `Workflow`.

        Converts a `dewret.workflow.Workflow` into a CWL-rendering object.

        Args:
            workflow: workflow to convert.
            name: name of this workflow, if it should have one.
        """
        display_name = name
        name = k8s_name(name)
        parameters: list[ParameterReference[Any] | FactoryCall] = [
            param for param in
            workflow.find_parameters(
                include_factory_calls=not get_render_configuration(
                    "factories_as_params"
                )
            )
            if not isinstance(param, DatasetParameter)
        ]

        if get_render_configuration("factories_as_params"):
            parameters += list(workflow.find_factories().values())

        pipeline_outputs = {}
        with BuilderPipeline(name or "myname") as dsl_pipeline:
            for step in workflow.indexed_steps.values():
                if isinstance(step, FactoryCall) and get_render_configuration(
                    "factories_as_params"
                ):
                    continue
                StepDefinition.from_step(step)
            pipeline_outputs = {
                dsl.component_factory.SINGLE_OUTPUT_NAME: ensure_channels(
                    workflow.result,
                    name
                )
            }

        inputs, outputs = get_name_to_specs([
            (param.name, with_type(param))
            for param in parameters
        ], with_type(workflow.result), step_name=name)

        description = "DESCRIPTION"
        component_name = "NAME"
        component_spec = dsl.structures.ComponentSpec(
            name=component_name,
            description=description,
            inputs=inputs,
            outputs=outputs,
            implementation=dsl.structures.Implementation(),
        )

        args_list = []
        for parameter in parameters:
            input_spec = component_spec.inputs[parameter.name]
            args_list.append(
                dsl.pipeline_channel.create_pipeline_channel(
                    name=parameter.name,
                    channel_type=input_spec.type,
                    is_artifact_list=input_spec.is_artifact_list,
                )
            )

        graph_component = cls(component_spec=component_spec)
        pipeline_group = dsl_pipeline.groups[0]
        pipeline_group.name = uuid.uuid4().hex

        pipeline_spec, platform_spec = builder.create_pipeline_spec(
            pipeline=dsl_pipeline,
            component_spec=graph_component.component_spec,
            pipeline_outputs=pipeline_outputs,
            pipeline_config={},
        )
        # pipeline_root = getattr(pipeline_func, 'pipeline_root', None)
        # if pipeline_root is not None:
        #     pipeline_spec.default_pipeline_root = pipeline_root
        if display_name is not None:
            pipeline_spec.pipeline_info.display_name = display_name
        if component_spec.description is not None:
            pipeline_spec.pipeline_info.description = component_spec.description

        graph_component.component_spec.implementation.graph = pipeline_spec
        graph_component.component_spec.platform_spec = platform_spec
        return graph_component

    @property
    def pipeline_spec(self) -> pipeline_spec_pb2.PipelineSpec:
        """Returns the pipeline spec of the component."""
        return self.component_spec.implementation.graph

    def execute(self, **kwargs: Any) -> None:
        raise RuntimeError("Graph component has no local execution mode.")

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        pipeline_spec_dict = json_format.MessageToDict(self.pipeline_spec)
        # yaml_comments = extract_comments_from_pipeline_spec(pipeline_spec_dict,
        #                                                     self.description)
        # has_platform_specific_features = len(self.platform_spec.platforms) > 0

        # documents = [pipeline_spec_dict]
        # if has_platform_specific_features:
        #     documents.append(json_format.MessageToDict(self.platform_spec))
        return yaml.safe_dump(pipeline_spec_dict, sort_keys=True)


def render(
    workflow: Workflow, **kwargs: Unpack[KubeflowRendererConfiguration]
) -> dict[str, dict[str, RawType]]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.
        **kwargs: additional configuration arguments - these should match KubeflowRendererConfiguration.

    Returns:
        Reduced form as a native Python dict structure for
        serialization.
    """
    # TODO: Again, convincing mypy that a TypedDict has RawType values.
    with set_render_configuration(kwargs):  # type: ignore
        rendered = base_render(
            workflow,
            lambda workflow: DewretGraphComponent.from_workflow(workflow).render(),
        )
    return rendered
