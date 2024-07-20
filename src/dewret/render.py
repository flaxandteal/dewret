import sys
import importlib
from pathlib import Path
from typing import Protocol, TypeVar, Any, Unpack, TypedDict
import yaml

from .workflow import Workflow
from .utils import RawType
from .workflow import Workflow

RenderConfiguration = TypeVar("RenderConfiguration", bound=dict[str, Any])

class RawRenderModule(Protocol):
    def render_raw(self, workflow: Workflow, **kwargs: RenderConfiguration) -> str | tuple[str, dict[str, str]]:
        ...

class StructuredRenderModule(Protocol):
    def render(self, workflow: Workflow, **kwargs: RenderConfiguration) -> RawType | tuple[str, dict[str, RawType]]:
        ...

def structured_to_raw(rendered: RawType, pretty: bool=False) -> str:
    if pretty:
        output = yaml.dumps(rendered, indent=2)
    else:
        output = str(rendered)
    return output

def get_render_method(renderer: Path | RawRenderModule | StructuredRenderModule, pretty: bool=False):
    render_module: RawRenderModule | StructuredRenderModule
    if isinstance(renderer, Path):
        if (render_dir := str(renderer.parent)) not in sys.path:
            sys.path.append(render_dir)
        loader = importlib.machinery.SourceFileLoader("renderer", str(renderer))
        render_module = loader.load_module()
    else:
        render_module = renderer
    if hasattr(render_module, "render_raw"):
        return render_module.render_raw

    def _render(workflow: Workflow, pretty=False, **kwargs: RenderConfiguration) -> str | tuple[str, dict[str, str]]:
        rendered = render_module.render(workflow, **kwargs)
        if isinstance(rendered, tuple) and len(rendered) == 2:
            return structured_to_raw({
                "__root__": rendered[0],
                **rendered[1]
            }, pretty=pretty)
        return structured_to_raw(rendered, pretty=pretty)

    return _render
