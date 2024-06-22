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

from attrs import define, has as attrs_has, fields as attrs_fields, AttrsInstance
from dataclasses import dataclass, is_dataclass, fields as dataclass_fields
from collections.abc import Mapping
from contextvars import ContextVar
from typing import TypedDict, NotRequired, get_args, Union, cast, Any, Unpack
from types import UnionType

from dewret.workflow import (
    FactoryCall,
    Reference,
    Raw,
    Workflow,
    BaseStep,
    NestedStep,
    StepReference,
    ParameterReference,
    Unset,
)
from dewret.utils import RawType, flatten, DataclassProtocol

InputSchemaType = Union[str, "CommandInputSchema", list[str], list["InputSchemaType"]]


@dataclass
class CWLRendererConfiguration(TypedDict):
    """Configuration for the renderer.

    Attributes:
        allow_complex_types: can input/output types be other than raw?
        factories_as_params: should factories be treated as input or steps?
    """

    allow_complex_types: NotRequired[bool]
    factories_as_params: NotRequired[bool]


CONFIGURATION: ContextVar[CWLRendererConfiguration] = ContextVar("cwl-configuration")


def set_configuration(configuration: CWLRendererConfiguration) -> None:
    """Set configuration for this rendering.

    Args:
        configuration: overridden settings as dict.
    """
    CONFIGURATION.set(
        CWLRendererConfiguration(
            allow_complex_types=False,
            factories_as_params=False,
        )
    )
    CONFIGURATION.get().update(configuration)


def configuration(key: str) -> Any:
    """Retrieve current configuration (thread/async-local)."""
    current_configuration = CONFIGURATION.get()
    if key not in current_configuration:
        raise KeyError("Unknown configuration settings.")
    return current_configuration.get(key)


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
        return {"source": self.source}


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
        return cls(
            name=step.name,
            run=step.task.name,
            out=(to_output_schema("out", step.return_type)["fields"])
            if attrs_has(step.return_type) or is_dataclass(step.return_type)
            else ["out"],
            in_={
                key: (
                    ReferenceDefinition.from_reference(param)
                    if isinstance(param, Reference)
                    else param
                )
                for key, param in step.arguments.items()
            },
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
                    if isinstance(ref, ReferenceDefinition)
                    else {"default": ref.value}
                )
                for key, ref in self.in_.items()
            },
            "out": flatten(self.out),
        }


def cwl_type_from_value(val: RawType | Unset) -> str | list[str]:
    """Find a CWL type for a given (possibly Unset) value.

    Args:
        val: a raw Python variable or an unset variable.

    Returns:
        Type as a string or list of strings.
    """
    if val is not None and hasattr(val, "__type__"):
        raw_type = val.__type__
    else:
        raw_type = type(val)

    return to_cwl_type(raw_type)


def to_cwl_type(typ: type) -> str | list[str]:
    """Map Python types to CWL types.

    Args:
        typ: a Python basic type.

    Returns:
        CWL specification type name, or a list
        if a union.
    """
    if isinstance(typ, UnionType):
        return [to_cwl_type(item) for item in get_args(typ)]

    if typ == int:
        return "int"
    elif typ == bool:
        return "boolean"
    elif typ == dict or attrs_has(typ):
        return "record"
    elif typ == list:
        return "array"
    elif typ == float:
        return "double"
    elif typ == str:
        return "string"
    else:
        if configuration("allow_complex_types"):
            return typ.__name__
        raise TypeError(f"Cannot render complex type ({typ}) to CWL")


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

    type: InputSchemaType
    label: str
    fields: NotRequired[dict[str, "CommandInputSchema"]]
    items: NotRequired[InputSchemaType]
    default: NotRequired[RawType]


class CommandOutputSchema(CommandInputSchema):
    """Structure for referring to an output in CWL.

    As a simplification, this is an input schema with an extra
    `outputSource` field.

    Attributes:
        outputSource: step result to use for this output.
    """

    outputSource: NotRequired[str]


def raw_to_command_input_schema(label: str, value: RawType | Unset) -> InputSchemaType:
    """Infer the CWL input structure for this value.

    Inspects the value, to work out an appropriate structure
    describing it in CWL.

    Args:
        label: name of the variable.
        value: basic-typed variable from which to build structure.

    Returns:
        Structure used to define (possibly compound) basic types for input.
    """
    if isinstance(value, dict) or isinstance(value, list):
        return _raw_to_command_input_schema_internal(label, value)
    else:
        return cwl_type_from_value(value)


def to_output_schema(
    label: str,
    typ: type[RawType | AttrsInstance | DataclassProtocol],
    output_source: str | None = None,
) -> CommandOutputSchema:
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
    if attrs_has(typ):
        fields = {
            str(field.name): cast(
                CommandInputSchema, to_output_schema(field.name, field.type)
            )
            for field in attrs_fields(typ)
        }
    elif is_dataclass(typ):
        fields = {
            str(field.name): cast(
                CommandInputSchema, to_output_schema(field.name, field.type)
            )
            for field in dataclass_fields(typ)
        }

    if fields:
        output = CommandOutputSchema(
            type="record",
            label=label,
            fields=fields,
        )
    else:
        output = CommandOutputSchema(
            type=to_cwl_type(typ),
            label=label,
        )
    if output_source is not None:
        output["outputSource"] = output_source
    return output


def _raw_to_command_input_schema_internal(
    label: str, value: RawType | Unset
) -> CommandInputSchema:
    typ = cwl_type_from_value(value)
    structure: CommandInputSchema = {"type": typ, "label": label}
    if isinstance(value, dict):
        structure["fields"] = {
            key: _raw_to_command_input_schema_internal(key, val)
            for key, val in value.items()
        }
    elif isinstance(value, list):
        typeset = set(get_args(value))
        if not typeset:
            typeset = {type(item) for item in value}
        if len(typeset) != 1:
            raise RuntimeError(
                "For CWL, an input array must have a consistent type, "
                "and we need at least one element to infer it, or an explicit typehint."
            )
        structure["items"] = to_cwl_type(typeset.pop())
    elif not isinstance(value, Unset):
        structure["default"] = value
    return structure


@define
class InputsDefinition:
    """CWL-renderable representation of an input parameter block.

    Turns dewret results into a CWL input block.

    Attributes:
        input: sequence of results from a workflow.
    """

    inputs: dict[str, "CommandInputParameter"]

    @define
    class CommandInputParameter:
        """CWL-renderable reference to a specific input.

        Attributes:
            type: type of variable
            name: fully-qualified name of the input.
        """

        type: InputSchemaType
        default: RawType | Unset
        label: str

    @classmethod
    def from_parameters(
        cls, parameters: list[ParameterReference | FactoryCall]
    ) -> "InputsDefinition":
        """Takes a list of parameters into a CWL structure.

        Uses the parameters to fill out the necessary input fields.

        Returns:
            CWL-like structure representing all workflow outputs.
        """
        return cls(
            inputs={
                input.name: cls.CommandInputParameter(
                    label=input.name,
                    default=input.default,
                    type=raw_to_command_input_schema(
                        label=input.name, value=input.default
                    ),
                )
                for input in parameters
            }
        )

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        result: dict[str, RawType] = {}
        for key, input in self.inputs.items():
            item = {
                # Would rather not cast, but CommandInputSchema is dict[RawType]
                # by construction, where type is seen as a TypedDict subclass.
                "type": cast(RawType, input.type),
                "label": input.label,
            }
            if not isinstance(input.default, Unset):
                item["default"] = input.default
            result[key] = item
        return result


@define
class OutputsDefinition:
    """CWL-renderable set of workflow outputs.

    Turns dewret results into a CWL output block.

    Attributes:
        outputs: sequence of results from a workflow.
    """

    outputs: dict[str, "CommandOutputSchema"]

    @classmethod
    def from_results(
        cls, results: dict[str, StepReference[Any]]
    ) -> "OutputsDefinition":
        """Takes a mapping of results into a CWL structure.

        Pulls the result type from the signature, ultimately, if possible.

        Returns:
            CWL-like structure representing all workflow outputs.
        """
        return cls(
            outputs={
                key: to_output_schema(
                    result.field, result.return_type, output_source=result.name
                )
                for key, result in results.items()
            }
        )

    def render(self) -> dict[str, RawType]:
        """Render to a dict-like structure.

        Returns:
            Reduced form as a native Python dict structure for
            serialization.
        """
        return {key: flatten(output) for key, output in self.outputs.items()}


@define
class WorkflowDefinition:
    """CWL-renderable workflow.

    Coerces the dewret structure of a workflow into that
    needed for valid CWL.

    Attributes:
        steps: sequence of steps in the workflow.
    """

    steps: list[StepDefinition]
    inputs: InputsDefinition
    outputs: OutputsDefinition
    name: None | str

    @classmethod
    def from_workflow(
        cls, workflow: Workflow, name: None | str = None
    ) -> "WorkflowDefinition":
        """Build from a `Workflow`.

        Converts a `dewret.workflow.Workflow` into a CWL-rendering object.

        Args:
            workflow: workflow to convert.
            name: name of this workflow, if it should have one.
        """
        parameters: list[ParameterReference | FactoryCall] = list(
            workflow.find_parameters(
                include_factory_calls=not configuration("factories_as_params")
            )
        )
        if configuration("factories_as_params"):
            parameters += list(workflow.find_factories().values())
        return cls(
            steps=[
                StepDefinition.from_step(step)
                for step in workflow.steps
                if not (
                    isinstance(step, FactoryCall)
                    and configuration("factories_as_params")
                )
            ],
            inputs=InputsDefinition.from_parameters(parameters),
            outputs=OutputsDefinition.from_results(
                {workflow.result.field: workflow.result} if workflow.result else {}
            ),
            name=name,
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
            "inputs": self.inputs.render(),
            "outputs": self.outputs.render(),
            "steps": {step.name: step.render() for step in self.steps},
        }


def render(
    workflow: Workflow, **kwargs: Unpack[CWLRendererConfiguration]
) -> dict[str, RawType] | tuple[dict[str, RawType], dict[str, dict[str, RawType]]]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.
        **kwargs: additional configuration arguments - these should match CWLRendererConfiguration.

    Returns:
        Reduced form as a native Python dict structure for
        serialization.
    """
    set_configuration(kwargs)
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
