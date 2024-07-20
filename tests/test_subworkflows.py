"""Check subworkflow behaviour is as expected."""

from typing import Callable
from queue import Queue
import yaml
from dewret.tasks import construct, subworkflow, task, factory
from dewret.renderers.cwl import render
from dewret.workflow import param

from ._lib.extra import increment, sum

CONSTANT = 3

QueueFactory: Callable[..., "Queue[int]"] = factory(Queue)

GLOBAL_QUEUE = QueueFactory()


@task()
def pop(queue: "Queue[int]") -> int:
    """Remove element of a queue."""
    return queue.get()


@task()
def to_int(num: int | float) -> int:
    """Cast to an int."""
    return int(num)


@task()
def add_and_queue(num: int, queue: "Queue[int]") -> "Queue[int]":
    """Add a global constant to a number."""
    queue.put(num)
    return queue


@subworkflow()
def make_queue(num: int | float) -> "Queue[int]":
    """Add a number to a queue."""
    queue = QueueFactory()
    return add_and_queue(num=to_int(num=num), queue=queue)


@subworkflow()
def get_global_queue(num: int | float) -> "Queue[int]":
    """Add a number to a global queue."""
    return add_and_queue(num=to_int(num=num), queue=GLOBAL_QUEUE)


@subworkflow()
def add_constant(num: int | float) -> int:
    """Add a global constant to a number."""
    return to_int(num=sum(left=num, right=CONSTANT))


def test_subworkflows_can_use_globals() -> None:
    """Produce a subworkflow that uses a global."""
    my_param = param("num", typ=int)
    result = increment(num=add_constant(num=increment(num=my_param)))
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow)
    rendered = subworkflows["__root__"]

    assert len(subworkflows) == 2
    assert isinstance(subworkflows, dict)

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          CONSTANT:
            label: CONSTANT
            default: 3
            type: int
          num:
            label: num
            type: int
        outputs:
          out:
            label: out
            outputSource: increment-2/out
            type: int
        steps:
          increment-1:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          increment-2:
             in:
               num:
                 source: add_constant-1/out
             out: [out]
             run: increment
          add_constant-1:
             in:
               CONSTANT:
                 source: CONSTANT
               num:
                 source: increment-1/out
             out: [out]
             run: add_constant
    """)


def test_subworkflows_can_use_factories() -> None:
    """Produce a subworkflow that uses a factory."""
    my_param = param("num", typ=int)
    result = pop(queue=make_queue(num=increment(num=my_param)))
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow, allow_complex_types=True)
    rendered = subworkflows["__root__"]

    assert len(subworkflows) == 2
    assert isinstance(subworkflows, dict)

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: int
        outputs:
          out:
            label: out
            outputSource: pop-1/out
            type: int
        steps:
          increment-1:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          make_queue-1:
             in:
               num:
                 source: increment-1/out
             out: [out]
             run: make_queue
          pop-1:
             in:
               queue:
                 source: make_queue-1/out
             out: [out]
             run: pop
    """)


def test_subworkflows_can_use_global_factories() -> None:
    """Check whether we can produce a subworkflow that uses a global factory."""
    my_param = param("num", typ=int)
    result = pop(queue=get_global_queue(num=increment(num=my_param)))
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow, allow_complex_types=True)
    rendered = subworkflows["__root__"]

    assert len(subworkflows) == 2
    assert isinstance(subworkflows, dict)

    assert rendered == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: int
        outputs:
          out:
            label: out
            outputSource: pop-1/out
            type: int
        steps:
          increment-1:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          get_global_queue-1:
             in:
               num:
                 source: increment-1/out
             out: [out]
             run: get_global_queue
          pop-1:
             in:
               queue:
                 source: get_global_queue-1/out
             out: [out]
             run: pop
    """)
