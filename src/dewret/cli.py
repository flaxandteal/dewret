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
import importlib.util
from pathlib import Path
from contextlib import contextmanager
import sys
import re
import yaml
from typing import Any, IO, Generator
from types import ModuleType
import click
import json

from .core import (
    set_configuration,
    set_render_configuration,
    RawRenderModule,
    StructuredRenderModule,
)
from .utils import load_module_or_package
from .render import get_render_method, write_rendered_output

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
@click.option("--construct-args", default="simplify_ids:true")
@click.option("--renderer", default="cwl")
@click.option("--renderer-args", default="")
@click.option("--output", default="-")
@click.argument("workflow_py", type=click.Path(exists=True, path_type=Path))
@click.argument("task")
@click.argument("arguments", nargs=-1)
def render(
    workflow_py: Path,
    task: str,
    arguments: list[str],
    pretty: bool,
    backend: Backend,
    construct_args: str,
    renderer: str,
    renderer_args: str,
    output: str,
) -> None:
    """Render a workflow.

    WORKFLOW_PY is the Python file containing workflow.
    TASK is the name of (decorated) task in workflow module.
    ARGUMENTS is zero or more pairs representing constant arguments to pass to the task, in the format `key:val` where val is a JSON basic type.
    """
    sys.path.append(str(workflow_py.parent))
    kwargs = {}
    for arg in arguments:
        if ":" not in arg:
            raise RuntimeError(
                "Arguments should be specified as key:val, where val is a JSON representation of the argument"
            )
        key, val = arg.split(":", 1)
        kwargs[key] = json.loads(val)

    render_module: Path | ModuleType
    if mtch := re.match(r"^([a-z_0-9-.]+)$", renderer):
        try:
            render_module = importlib.import_module(mtch.group(1))
        except ModuleNotFoundError:
            try:
                render_module = importlib.import_module(
                    f"dewret.renderers.{mtch.group(1)}"
                )
            except ModuleNotFoundError as err:
                raise ModuleNotFoundError(
                    f"Unable to find render module {mtch.group(1)} in PYTHONPATH or dewret.renderers."
                ) from err
        if not isinstance(render_module, RawRenderModule) and not isinstance(
            render_module, StructuredRenderModule
        ):
            raise NotImplementedError(
                "The imported render module does not seem to match the `RawRenderModule` or `StructuredRenderModule` protocols."
            )
    elif renderer.startswith("@"):
        render_module = Path(renderer[1:])
    else:
        raise RuntimeError(
            "Renderer argument should be a known dewret renderer, or '@FILENAME' where FILENAME is a renderer"
        )

    if construct_args.startswith("@"):
        with Path(construct_args[1:]).open() as construct_args_f:
            construct_kwargs = yaml.safe_load(construct_args_f)
    elif not construct_args:
        construct_kwargs = {}
    else:
        construct_kwargs = dict(pair.split(":") for pair in construct_args.split(","))

    renderer_kwargs: dict[str, Any]
    if renderer_args.startswith("@"):
        with Path(renderer_args[1:]).open() as renderer_args_f:
            renderer_kwargs = yaml.safe_load(renderer_args_f)
    elif not renderer_args:
        renderer_kwargs = {}
    else:
        renderer_kwargs = dict(pair.split(":") for pair in renderer_args.split(","))

    if output == "-":

        @contextmanager
        def _opener(key: str, mode: str) -> Generator[IO[Any], None, None]:
            print(" ------ ", key, " ------ ")
            yield sys.stdout
            print()

        opener = _opener
    else:

        @contextmanager
        def _opener(key: str, mode: str) -> Generator[IO[Any], None, None]:
            output_file = output.replace("%", key)
            with Path(output_file).open(mode) as output_f:
                yield output_f

        opener = _opener

    render = get_render_method(render_module, pretty=pretty)
    pkg = "__workflow__"
    workflow = load_module_or_package(pkg, workflow_py)
    task_fn = getattr(workflow, task)

    try:
        with (
            set_configuration(**construct_kwargs),
            set_render_configuration(renderer_kwargs),
        ):
            rendered = render(
                construct(task_fn(**kwargs), **construct_kwargs), **renderer_kwargs
            )
    except Exception as exc:
        import traceback

        print(exc, exc.__cause__, exc.__context__)
        traceback.print_exc()
    else:
        write_rendered_output(rendered, output, opener)
