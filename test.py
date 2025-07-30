import sys
import yaml
from dewret import render
from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from dewret.annotations import AtRender


@task()
def cb(inp: str) -> str:
    # Any side-effect logic
    pass


@task()
def concat_strings(str1: str, str2: str) -> str:
    return str1 + str2


concat_strings._obj.after(cb)


@workflow()
def create_greeting_in_all_caps(prefix: AtRender[str], name: AtRender[str]) -> str:
    prefix_cap = prefix.capitalize()
    name_cap = name.capitalize()
    return concat_strings(str1=prefix_cap, str2=name_cap)


result = create_greeting_in_all_caps(prefix="Hello to ", name="John")
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
yaml.dump(cwl, sys.stdout, indent=2)
