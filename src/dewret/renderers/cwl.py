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
from dataclasses import is_dataclass, fields as dataclass_fields
from collections.abc import Mapping
from typing import (
    TypedDict,
    NotRequired,
    get_origin,
    get_args,
    cast,
    Any,
    Unpack,
    Iterable,
    Type,
    TypeVar,
)
from types import UnionType
from inspect import isclass
from sympy import Basic, Tuple, Dict, jscode, Symbol

from dewret.core import (
    Raw,
    RawType,
    FirmType,
)
from dewret.workflow import (
    FactoryCall,
    Workflow,
    BaseStep,
    StepReference,
    ParameterReference,
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
from dewret.core import Reference, get_render_configuration, set_render_configuration


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


T = TypeVar("T")

InputSchemaType = (
    str
    | CommandInputSchema
    | list[str]
    | list["InputSchemaType"]
    | dict[str, "str | InputSchemaType"]
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


class CWLRendererConfiguration(TypedDict):
    """Configuration for the renderer.

    Attributes:
        allow_complex_types: can input/output types be other than raw?
        factories_as_params: should factories be treated as input or steps?
    """

    allow_complex_types: NotRequired[bool]
    factories_as_params: NotRequired[bool]


def default_config() -> CWLRendererConfiguration:
    """Default configuration for this renderer.

    This is a hook-like call to give a configuration dict that this renderer
    will respect, and sets any necessary default values.

    Returns: a dict with (preferably) raw type structures to enable easy setting
        from YAML/JSON.
    """
    return {
        "allow_complex_types": False,
        "factories_as_params": False,
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
        out: list[str] | dict[str, "CommandInputSchema"]
        if attrs_has(step.return_type) or (
            is_dataclass(step.return_type) and isclass(step.return_type)
        ):
            out = to_output_schema("out", step.return_type)["fields"]
        else:
            out = ["out"]
        return cls(
            name=step.name,
            run=step.task.name,
            out=out,
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


def cwl_type_from_value(label: str, val: RawType | Unset) -> CommandInputSchema:
    """Find a CWL type for a given (possibly Unset) value.

    Args:
        label: the label for the variable being checked to prefill the input def and improve debugging info.
        val: a raw Python variable or an unset variable.

    Returns:
        Input schema type.
    """
    if val is not None and hasattr(val, "__type__"):
        raw_type = val.__type__
    else:
        raw_type = type(val)

    return to_cwl_type(label, raw_type)


def to_cwl_type(label: str, typ: Type[T]) -> CommandInputSchema:
    """Map Python types to CWL types.

    Args:
        label: the label for the variable being checked to prefill the input def and improve debugging info.
        typ: a Python basic type.

    Returns:
        CWL specification type dict.
    """
    typ_dict: CommandInputSchema = {"label": label, "type": ""}
    base: Any | None = typ
    args = get_args(typ)
    if args:
        base = get_origin(typ)

    if base == type(None):
        typ_dict["type"] = "null"
    elif base == int:
        typ_dict["type"] = "int"
    elif base == bool:
        typ_dict["type"] = "boolean"
    elif base == dict or (isinstance(base, type) and attrs_has(base)):
        typ_dict["type"] = "record"
    elif base == float:
        typ_dict["type"] = "float"
    elif base == str:
        typ_dict["type"] = "string"
    elif base == bytes:
        typ_dict["type"] = "bytes"
    elif isinstance(typ, UnionType):
        typ_dict.update(
            {"type": tuple(to_cwl_type(label, item)["type"] for item in args)}
        )
    elif isclass(base) and issubclass(base, Iterable):
        try:
            if len(args) > 1:
                typ_dict.update(
                    {
                        "type": "array",
                        "items": [to_cwl_type(label, t)["type"] for t in args],
                    }
                )
            elif len(args) == 1:
                typ_dict.update(
                    {"type": "array", "items": to_cwl_type(label, args[0])["type"]}
                )
            else:
                typ_dict["type"] = "array"
        except IndexError as err:
            raise TypeError(
                f"Cannot render complex type ({typ}) to CWL for {label}, have you enabled allow_complex_types configuration?"
            ) from err
    elif get_render_configuration("allow_complex_types"):
        typ_dict["type"] = typ if isinstance(typ, str) else typ.__name__
    else:
        raise TypeError(f"Cannot render type ({typ}) to CWL for {label}")
    return typ_dict


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
        return cwl_type_from_value(label, value)


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
        fields = {}
        for field in dataclass_fields(typ):
            # Ideally would raise a type error if the dataclass fields are not valid
            # for to_output_schema, but given that we have no simple, 3.11-compatible
            # way of accepting things like generics and forward references, this turns
            # out to be non-trivial.
            #  raise TypeError("Types of fields in results must also be valid result-types themselves (string-defined types not currently allowed)")
            fields[str(field.name)] = cast(
                CommandInputSchema,
                to_output_schema(field.name, field.type),  # type: ignore
            )
    if fields:
        output = CommandOutputSchema(
            type="record",
            label=label,
            fields=fields,
        )
    else:
        # TODO: this complains because NotRequired keys are never present,
        # but that does not seem like a problem here - likely a better solution.
        output = CommandOutputSchema(
            **to_cwl_type(label, typ)  # type: ignore
        )
    if output_source is not None:
        output["outputSource"] = output_source
    return output


def _raw_to_command_input_schema_internal(
    label: str, value: RawType | Unset
) -> CommandInputSchema:
    structure: CommandInputSchema = cwl_type_from_value(label, value)
    if isinstance(value, dict):
        structure["fields"] = {
            key: _raw_to_command_input_schema_internal(key, val)
            for key, val in value.items()
        }
    elif isinstance(value, list):
        typeset = set(get_args(value))
        if not typeset:
            typeset = {
                item.__type__
                if item is not None and hasattr(item, "__type__")
                else type(item)
                for item in value
            }
        if len(typeset) != 1:
            raise RuntimeError(
                "For CWL, an input array must have a consistent type, "
                "and we need at least one element to infer it, or an explicit typehint."
            )
        structure["items"] = to_cwl_type(label, typeset.pop())["type"]
    elif not isinstance(value, Unset):
        structure["default"] = firm_to_raw(value)
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
        cls, parameters: list[ParameterReference[Any] | FactoryCall]
    ) -> "InputsDefinition":
        """Takes a list of parameters into a CWL structure.

        Uses the parameters to fill out the necessary input fields.

        Returns:
            CWL-like structure representing all workflow outputs.
        """
        parameters_dedup = {
            p._.parameter for p in parameters if isinstance(p, ParameterReference)
        }
        parameters = list(parameters_dedup) + [
            p for p in parameters if not isinstance(p, ParameterReference)
        ]
        return cls(
            inputs={
                input.name: cls.CommandInputParameter(
                    label=input.__name__,
                    default=(default := flatten_if_set(input.__default__)),
                    type=raw_to_command_input_schema(
                        label=input.__original_name__, value=default
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
            # Would rather not cast, but CommandInputSchema is dict[RawType]
            # by construction, where type is seen as a TypedDict subclass.
            item = firm_to_raw(cast(FirmType, input.type))
            if isinstance(item, dict) and not isinstance(input.default, Unset):
                item["default"] = firm_to_raw(input.default)
            result[key] = item
        return result


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
                return to_output_schema(  # type: ignore
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
        parameters: list[ParameterReference[Any] | FactoryCall] = list(
            workflow.find_parameters(
                include_factory_calls=not get_render_configuration(
                    "factories_as_params"
                )
            )
        )
        if get_render_configuration("factories_as_params"):
            parameters += list(workflow.find_factories().values())
        return cls(
            steps=[
                StepDefinition.from_step(step)
                for step in workflow.indexed_steps.values()
                if not (
                    isinstance(step, FactoryCall)
                    and get_render_configuration("factories_as_params")
                )
            ],
            inputs=InputsDefinition.from_parameters(parameters),
            outputs=OutputsDefinition.from_results(
                workflow.result
                if isinstance(workflow.result, list | tuple | Tuple)
                else {with_field(workflow.result): workflow.result}
                if workflow.has_result and workflow.result is not None
                else {}
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
) -> dict[str, dict[str, RawType]]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.
        **kwargs: additional configuration arguments - these should match CWLRendererConfiguration.

    Returns:
        Reduced form as a native Python dict structure for
        serialization.
    """
    # TODO: Again, convincing mypy that a TypedDict has RawType values.
    with set_render_configuration(kwargs):  # type: ignore
        rendered = base_render(
            workflow,
            lambda workflow: WorkflowDefinition.from_workflow(workflow).render(),
        )
    return rendered
