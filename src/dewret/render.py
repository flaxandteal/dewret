import sys
from pathlib import Path
from functools import partial
from typing import TypeVar, Callable, cast
import yaml

from .workflow import Workflow, NestedStep
from .core import RawType, RenderCall, BaseRenderModule, RawRenderModule, StructuredRenderModule, RenderConfiguration
from .utils import load_module_or_package

T = TypeVar("T")

def structured_to_raw(rendered: RawType, pretty: bool=False) -> str:
    """Serialize a serializable structure to a string.

    Args:
        rendered: a possibly-nested, static basic Python structure.
        pretty: whether to attempt YAML dumping with an indent of 2.

    Returns: YAML/stringified version of the structure.
    """
    if pretty:
        output = yaml.safe_dump(rendered, indent=2)
    else:
        output = str(rendered)
    return output

def get_render_method(renderer: Path | RawRenderModule | StructuredRenderModule, pretty: bool=False) -> RenderCall:
    """Create a ready-made callable to render the workflow that is appropriate for the renderer module.

    Args:
        renderer: a module or path to a module.
        pretty: whether the renderer should attempt to YAML-format the output (if relevant).

    Returns: a callable with a consistent interface, regardless of the renderer type.
    """
    render_module: BaseRenderModule
    if isinstance(renderer, Path):
        if (render_dir := str(renderer.parent)) not in sys.path:
            sys.path.append(render_dir)

        # Attempt to load renderer as package, falling back to a single module otherwise.
        # This enables relative imports in renderers and therefore the ability to modularize.
        module = load_module_or_package("__renderer__", renderer)
        sys.modules["__renderer_mod__"] = module
        render_module = cast(BaseRenderModule, module)
    else:
        render_module = renderer
    if not isinstance(render_module, StructuredRenderModule):
        if not isinstance(render_module, RawRenderModule):
            raise NotImplementedError("This render module neither seems to be a structured nor a raw render module.")
        return render_module.render_raw

    def _render(workflow: Workflow, render_module: StructuredRenderModule, pretty=False, **kwargs: RenderConfiguration) -> dict[str, str]:
        rendered = render_module.render(workflow, **kwargs)
        return {
            key: structured_to_raw(value, pretty=pretty)
            for key, value in rendered.items()
        }

    return cast(RenderCall, partial(_render, render_module=render_module, pretty=pretty))

def base_render(
    workflow: Workflow, build_cb: Callable[[Workflow], T]
) -> dict[str, T]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.
        build_cb: a callback to call for each workflow found.

    Returns:
        Reduced form as a native Python dict structure for
        serialization.
    """
    primary_workflow = build_cb(workflow)
    subworkflows = {}
    for step in workflow.indexed_steps.values():
        if isinstance(step, NestedStep):
            nested_subworkflows = base_render(step.subworkflow, build_cb)
            subworkflows.update(nested_subworkflows)
            subworkflows[step.name] = nested_subworkflows["__root__"]
    subworkflows["__root__"] = primary_workflow
    return subworkflows
