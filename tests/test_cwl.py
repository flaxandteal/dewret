import yaml
from dewret.tasks import task
from dewret.renderers.cwl import render
from dewret.utils import hasher

@task()
def increment(num: int):
    return num + 1

def test_cwl():
    rendered = render(increment(num=3))
    hsh = hasher(('increment', ('num', 'int|3')))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        steps:
          increment-{hsh}:
            run: increment
            in:
                num:
                    default: 3
    """)
