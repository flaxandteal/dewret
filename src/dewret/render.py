import sys
import importlib
from pathlib import Path
from functools import partial
from typing import Protocol, TypeVar, Any, Unpack, TypedDict, Callable
import yaml

from .workflow import Workflow, NestedStep
from .core import RawType
from .workflow import Workflow
from .utils import load_module_or_package

RenderConfiguration = TypeVar("RenderConfiguration", bound=dict[str, Any])

class RawRenderModule(Protocol):
    def render_raw(self, workflow: Workflow, **kwargs: RenderConfiguration) -> dict[str, str]:
        ...

class StructuredRenderModule(Protocol):
    def render(self, workflow: Workflow, **kwargs: RenderConfiguration) -> dict[str, dict[str, RawType]]:
        ...

def structured_to_raw(rendered: RawType, pretty: bool=False) -> str:
    if pretty:
        output = yaml.safe_dump(rendered, indent=2)
    else:
        output = str(rendered)
    return output

def get_render_method(renderer: Path | RawRenderModule | StructuredRenderModule, pretty: bool=False):
    render_module: RawRenderModule | StructuredRenderModule
    if isinstance(renderer, Path):
        if (render_dir := str(renderer.parent)) not in sys.path:
            sys.path.append(render_dir)

        # Attempt to load renderer as package, falling back to a single module otherwise.
        # This enables relative imports in renderers and therefore the ability to modularize.
        render_module = load_module_or_package("__renderer__", renderer)
        sys.modules["__renderer_mod__"] = render_module
    else:
        render_module = renderer
    if hasattr(render_module, "render_raw"):
        return render_module.render_raw

    def _render(workflow: Workflow, render_module: StructuredRenderModule, pretty=False, **kwargs: RenderConfiguration) -> dict[str, str]:
        rendered = render_module.render(workflow, **kwargs)
        return {
            key: structured_to_raw(value, pretty=pretty)
            for key, value in rendered.items()
        }

    return partial(_render, render_module=render_module, pretty=pretty)

T = TypeVar("T")
def base_render(
    workflow: Workflow, build_cb: Callable[[Workflow], T]
) -> dict[str, T]:
    """Render to a dict-like structure.

    Args:
        workflow: workflow to evaluate result.
        **kwargs: additional configuration arguments - these should match CWLRendererConfiguration.

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
