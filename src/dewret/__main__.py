import importlib
import yaml
import sys
import click
import json

from .renderers.cwl import render as cwl_render

@click.command()
@click.option("--pretty", is_flag=True, show_default=True, default=False, help="Pretty-print output where possible.")
@click.argument("workflow_py")
@click.argument("task")
@click.argument("arguments", nargs=-1)
def render(workflow_py: str, task: str, arguments: list[str], pretty: bool) -> None:
    """Render a workflow.

    WORKFLOW_PY is the Python file containing workflow.
    TASK is the name of (decorated) task in workflow module.
    ARGUMENTS is zero or more pairs representing constant arguments to pass to the task, in the format `key:val` where val is a JSON basic type.
    """

    loader = importlib.machinery.SourceFileLoader("workflow", workflow_py)
    workflow = loader.load_module()
    task_fn = getattr(workflow, task)
    kwargs = {}
    for arg in arguments:
        if ":" not in arg:
            raise RuntimeError("Arguments should be specified as key:val, where val is a JSON representation of the argument")
        key, val = arg.split(":", 1)
        kwargs[key] = json.loads(val)

    cwl = cwl_render(task_fn(**kwargs))
    if pretty:
        yaml.dump(cwl, sys.stdout, indent=2)
    else:
        print(cwl)

render()
