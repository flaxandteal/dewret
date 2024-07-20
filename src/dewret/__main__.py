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

"""CLI for dewret.

Simple CLI for running a workflow (dynamic->static) conversion. It is
more likely to use this tool programmatically, but for CI and toy examples,
this may be of use.
"""

import importlib
from pathlib import Path
import re
import yaml
from typing import Any
import sys
import click
import json

from .render import get_render_method, RawRenderModule, StructuredRenderModule
from .tasks import Backend, construct


@click.command()
@click.option(
    "--pretty",
    is_flag=True,
    show_default=True,
    default=False,
    help="Pretty-print output where possible.",
)
@click.option(
    "--backend",
    type=click.Choice(list(Backend.__members__)),
    show_default=True,
    default=Backend.DASK.name,
    help="Backend to use for workflow evaluation.",
)
@click.option(
    "--renderer",
    default="cwl"
)
@click.option(
    "--renderer-args",
    default="simplify_ids:true"
)
@click.argument("workflow_py")
@click.argument("task")
@click.argument("arguments", nargs=-1)
def render(
    workflow_py: str, task: str, arguments: list[str], pretty: bool, backend: Backend, renderer: str, renderer_args: str
) -> None:
    """Render a workflow.

    WORKFLOW_PY is the Python file containing workflow.
    TASK is the name of (decorated) task in workflow module.
    ARGUMENTS is zero or more pairs representing constant arguments to pass to the task, in the format `key:val` where val is a JSON basic type.
    """
    sys.path.append(str(Path(workflow_py).parent))
    loader = importlib.machinery.SourceFileLoader("workflow", workflow_py)
    workflow = loader.load_module()
    task_fn = getattr(workflow, task)
    kwargs = {}
    for arg in arguments:
        if ":" not in arg:
            raise RuntimeError(
                "Arguments should be specified as key:val, where val is a JSON representation of the argument"
            )
        key, val = arg.split(":", 1)
        kwargs[key] = json.loads(val)

    render_module: Path | RawRenderModule | StructuredRenderModule
    if (mtch := re.match(r"^([a-z_0-9-.]+)$", renderer)):
        render_module = importlib.import_module(f"dewret.renderers.{mtch.group(1)}")
    elif renderer.startswith("@"):
        render_module = Path(renderer[1:])
    else:
        raise RuntimeError("Renderer argument should be a known dewret renderer, or '@FILENAME' where FILENAME is a renderer")

    renderer_kwargs: dict[str, Any]
    if renderer_args.startswith("@"):
        with Path(renderer_args[1:]).open() as renderer_args_f:
            renderer_kwargs = yaml.load(renderer_args_f)
    elif not renderer_args:
        renderer_kwargs = {}
    else:
        renderer_kwargs = dict(pair.split(":") for pair in renderer_args.split(","))

    render = get_render_method(render_module, pretty=pretty)
    try:
        rendered = render(construct(task_fn(**kwargs), **renderer_kwargs))
    except Exception as exc:
        import traceback

        print(exc, exc.__cause__, exc.__context__)
        traceback.print_exc()
    else:
        if len(rendered) == 1:
            print(rendered["__root__"])
        else:
            print(rendered)


render()
