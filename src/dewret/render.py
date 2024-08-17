import sys
import importlib
from pathlib import Path
from functools import partial
from typing import Protocol, TypeVar, Any, Unpack, TypedDict, Callable
import yaml

from .workflow import Workflow, NestedStep
from .core import RawType
from .workflow import Workflow

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
        package_init = renderer.parent

        # Attempt to load renderer as package, falling back to a single module otherwise.
        # This enables relative imports in renderers and therefore the ability to modularize.
        try:
            loader = importlib.machinery.SourceFileLoader("renderer", str(package_init / "__init__.py"))
            sys.modules["renderer"] = loader.load_module(f"renderer")
            render_module = importlib.import_module(f"renderer.{renderer.stem}", "renderer")
        except ImportError:
            loader = importlib.machinery.SourceFileLoader("renderer", str(renderer))
            render_module = loader.load_module()
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
    for step in workflow.steps:
        if isinstance(step, NestedStep):
            nested_subworkflows = base_render(step.subworkflow, build_cb)
            subworkflows.update(nested_subworkflows)
            subworkflows[step.name] = nested_subworkflows["__root__"]
    subworkflows["__root__"] = primary_workflow
    return subworkflows
