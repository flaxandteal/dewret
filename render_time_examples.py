from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow

from dewret.annotations import AtRender

ONE: AtRender[int] = 1


@task()
def two(arg: int) -> int:
    return 2


def four(arg: int) -> int:
    return 4


from render_time_function_to_import import CONSTANT


@workflow()
def toy() -> list[int]:
    from render_time_function_to_import import one

    _1 = one(ONE)
    _2 = two(arg=ONE)

    def three(arg: int) -> int:
        return 1

    _3 = three(ONE)

    # _4 below doesn't work and no amount of annotations will make it work, render time functions
    # (i.e. those undecorated by dewret) can ONLY be imported within a @workflow from another file
    # it seems that given that they can only run at render time this importing restriction is confusing to me
    # Question: give a good example of why this is a good idea
    #######
    # _4 = four(ONE)

    # This one gets rendered and runs at run-time
    _5 = two(arg=CONSTANT)

    # I would expect these two not to work, because they are not annotated with AtRender but (conveniently?) they do
    _6 = one(1)
    _7 = three(1)

    _8 = two(arg=1)

    return [_1, _2, _3, _5, _6, _7, _8]


result = toy()
workflow = construct(result, simplify_ids=True)
cwl = render(workflow)
print(cwl["__root__"])

# python render_time_examples.py > render_time_examples.yaml
