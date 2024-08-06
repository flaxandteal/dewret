"""Check subworkflow behaviour is as expected."""

from typing import Callable
from queue import Queue
import yaml
from dewret.tasks import construct, subworkflow, task, factory, set_configuration
from dewret.renderers.cwl import render
from dewret.workflow import param

from ._lib.extra import increment, sum, pi

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
def get_global_queues(num: int | float) -> list["Queue[int] | int"]:
    """Add a number to a global queue."""
    return [
        add_and_queue(num=to_int(num=num), queue=GLOBAL_QUEUE),
        add_constant(num=num)
    ]


@subworkflow()
def add_constant(num: int | float) -> int:
    """Add a global constant to a number."""
    return to_int(num=sum(left=num, right=CONSTANT))

@subworkflow()
def add_constants(num: int | float) -> int:
    """Add a global constant to a number."""
    return to_int(num=sum(left=sum(left=num, right=CONSTANT), right=CONSTANT))

@subworkflow()
def get_values(num: int | float) -> tuple[int | float, int]:
    """Add a global constant to a number."""
    return (sum(left=num, right=CONSTANT), add_constant(CONSTANT))


def test_cwl_for_pairs() -> None:
    """Check whether we can produce CWL of pairs."""

    @subworkflow()
    def pair_pi() -> tuple[float, float]:
        return pi(), pi()

    with set_configuration(flatten_all_nested=True):
      result = pair_pi()
      workflow = construct(result, simplify_ids=True)
    rendered = render(workflow)["__root__"]

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs: [
          {{
            label: out,
            outputSource: pi-1/out,
            type: double
          }},
          {{
            label: out,
            outputSource: pi-1/out,
            type: double
          }}
        ]
        steps:
          pi-1:
            run: pi
            in: {{}}
            out: [out]
    """)


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


def test_subworkflows_can_return_lists() -> None:
    """Check whether we can produce a subworkflow that returns a list."""
    my_param = param("num", typ=int)
    result = get_global_queues(num=increment(num=my_param))
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow, allow_complex_types=True)
    rendered = subworkflows["__root__"]
    del subworkflows["__root__"]

    assert len(subworkflows) == 2
    assert isinstance(subworkflows, dict)
    osubworkflows = sorted(list(subworkflows.items()))

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
            outputSource: get_global_queues-1/out
            type: array
        steps:
          increment-1:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          get_global_queues-1:
             in:
               num:
                 source: increment-1/out
             out: [out]
             run: get_global_queues
    """)

    assert osubworkflows[0] == ("add_constant-1-1", yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: int
          CONSTANT:
            label: CONSTANT
            type: int
        outputs:
          out:
            label: out
            outputSource: to_int-1-1-1/out
            type: int
        steps:
          sum-1-1-1:
            in:
              left:
                source: num
              right:
                source: CONSTANT
            out:
            - out
            run: sum
          to_int-1-1-1:
            in:
              num:
                source: sum-1-1-1/out
            out:
            - out
            run: to_int
    """))

    assert osubworkflows[1] == ("get_global_queues-1", yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          CONSTANT:
            default: 3
            label: CONSTANT
            type: int
          num:
            label: num
            type: int
        outputs:
          - label: out
            outputSource: add_and_queue-1-1/out
            type: Queue[int]
          - label: out
            outputSource: add_constant-1-1/out
            type: int
        steps:
          Queue-1-1:
            in: {}
            out:
            - out
            run: Queue
          add_and_queue-1-1:
            in:
              num:
                source: to_int-1-1/out
              queue:
                source: Queue-1-1/out
            out:
            - out
            run: add_and_queue
          add_constant-1-1:
            in:
              CONSTANT:
                source: CONSTANT
              num:
                source: num
            out:
            - out
            run: add_constant
          to_int-1-1:
            in:
              num:
                source: num
            out:
            - out
            run: to_int
    """))

def test_can_merge_workflows() -> None:
    """Check whether we can merge workflows."""
    my_param = param("num", typ=int)
    value = to_int(num=increment(num=my_param))
    result = sum(left=value, right=increment(num=value))
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow, allow_complex_types=True)
    rendered = subworkflows["__root__"]
    del subworkflows["__root__"]

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
            outputSource: sum-1/out
            type: [
              int,
              double
            ]
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
                 source: to_int-1/out
             out: [out]
             run: increment
          sum-1:
             in:
               left:
                 source: to_int-1/out
               right:
                 source: increment-2/out
             out: [out]
             run: sum
          to_int-1:
             in:
               num:
                 source: increment-1/out
             out: [out]
             run: to_int
    """)


def test_subworkflows_can_use_globals_in_right_scope() -> None:
    """Produce a subworkflow that uses a global."""
    my_param = param("num", typ=int)
    result = increment(num=add_constants(num=increment(num=my_param)))
    workflow = construct(result, simplify_ids=True)
    subworkflows = render(workflow)
    rendered = subworkflows["__root__"]
    del subworkflows["__root__"]

    assert len(subworkflows) == 1
    assert isinstance(subworkflows, dict)
    osubworkflows = sorted(list(subworkflows.items()))

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
                 source: add_constants-1/out
             out: [out]
             run: increment
          add_constants-1:
             in:
               CONSTANT:
                 source: CONSTANT
               num:
                 source: increment-1/out
             out: [out]
             run: add_constants
    """)

    assert osubworkflows[0] == ("add_constants-1", yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: int
          CONSTANT:
            label: CONSTANT
            type: int
        outputs:
          out:
            label: out
            outputSource: to_int-1-1/out
            type: int
        steps:
          sum-1-1:
            in:
              left:
                source: num
              right:
                source: CONSTANT
            out:
            - out
            run: sum
          sum-1-2:
            in:
              left:
                source: sum-1-1/out
              right:
                source: CONSTANT
            out:
            - out
            run: sum
          to_int-1-1:
            in:
              num:
                source: sum-1-2/out
            out:
            - out
            run: to_int
    """))
