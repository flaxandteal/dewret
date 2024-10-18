"""Check subworkflow behaviour is as expected."""

from typing import Callable
from queue import Queue
import yaml
from dewret.tasks import construct, workflow, task, factory
from dewret.core import set_configuration
from dewret.renderers.cwl import render
from dewret.workflow import param

from ._lib.extra import increment, sum, pi, PackResult

CONSTANT: int = 3

QueueFactory: Callable[..., Queue[int]] = factory(Queue)

GLOBAL_QUEUE: Queue[int] = QueueFactory()


@task()
def pop(queue: Queue[int]) -> int:
    """Remove element of a queue."""
    return queue.get()


@task()
def to_int(num: int | float) -> int:
    """Cast to an int."""
    return int(num)


@task()
def add_and_queue(num: int, queue: Queue[int]) -> Queue[int]:
    """Add a global constant to a number."""
    queue.put(num)
    return queue


@workflow()
def make_queue(num: int | float) -> Queue[int]:
    """Add a number to a queue."""
    queue = QueueFactory()
    return add_and_queue(num=to_int(num=num), queue=queue)


@workflow()
def get_global_queue(num: int | float) -> Queue[int]:
    """Add a number to a global queue."""
    return add_and_queue(num=to_int(num=num), queue=GLOBAL_QUEUE)


@workflow()
def get_global_queues(num: int | float) -> list[Queue[int] | int]:
    """Add a number to a global queue."""
    return [
        add_and_queue(num=to_int(num=num), queue=GLOBAL_QUEUE),
        add_constant(num=num),
    ]


@workflow()
def add_constant(num: int | float) -> int:
    """Add a global constant to a number."""
    return to_int(num=sum(left=num, right=CONSTANT))


@workflow()
def add_constants(num: int | float) -> int:
    """Add a global constant to a number."""
    return to_int(num=sum(left=sum(left=num, right=CONSTANT), right=CONSTANT))


@workflow()
def get_values(num: int | float) -> tuple[int | float, int]:
    """Add a global constant to a number."""
    return (sum(left=num, right=CONSTANT), add_constant(CONSTANT))


def test_cwl_for_pairs() -> None:
    """Check whether we can produce CWL of pairs."""

    @workflow()
    def pair_pi() -> tuple[float, float]:
        return pi(), pi()

    with set_configuration(flatten_all_nested=True):
        result = pair_pi()
        wkflw = construct(result, simplify_ids=True)
    rendered = render(wkflw)["__root__"]

    assert rendered == yaml.safe_load("""
        cwlVersion: 1.2
        class: Workflow
        inputs: {}
        outputs: [
          {
            label: out,
            outputSource: pi-1/out,
            type: float
          },
          {
            label: out,
            outputSource: pi-1/out,
            type: float
          }
        ]
        steps:
          pi-1:
            run: pi
            in: {}
            out: [out]
    """)


def test_subworkflows_can_use_globals() -> None:
    """Produce a subworkflow that uses a global."""
    my_param = param("num", typ=int)
    result = increment(num=add_constant(num=increment(num=my_param)))
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw)
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
            outputSource: increment-1/out
            type: int
        steps:
          increment-2:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          increment-1:
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
                 source: increment-2/out
             out: [out]
             run: add_constant
    """)


def test_subworkflows_can_use_factories() -> None:
    """Produce a subworkflow that uses a factory."""
    my_param = param("num", typ=int)
    result = pop(queue=make_queue(num=increment(num=my_param)))
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw, allow_complex_types=True)
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
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw, allow_complex_types=True)
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
          GLOBAL_QUEUE:
            label: GLOBAL_QUEUE
            type: Queue
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
               GLOBAL_QUEUE:
                 source: GLOBAL_QUEUE
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
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw, allow_complex_types=True)
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
          CONSTANT:
            label: CONSTANT
            default: 3
            type: int
          GLOBAL_QUEUE:
            label: GLOBAL_QUEUE
            type: Queue
        outputs:
          out:
            label: out
            items:
            - Queue
            - int
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
               CONSTANT:
                 source: CONSTANT
               GLOBAL_QUEUE:
                 source: GLOBAL_QUEUE
             out: [out]
             run: get_global_queues
    """)

    assert osubworkflows[0] == (
        "add_constant-1-1",
        yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: int
          CONSTANT:
            default: 3
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
    """),
    )

    assert osubworkflows[1] == (
        "get_global_queues-1",
        yaml.safe_load("""
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
          GLOBAL_QUEUE:
            label: GLOBAL_QUEUE
            type: Queue
        outputs:
          - label: out
            outputSource: add_and_queue-1-1/out
            type: Queue
          - label: out
            outputSource: add_constant-1-1/out
            type: int
        steps:
          add_and_queue-1-1:
            in:
              num:
                source: to_int-1-1/out
              queue:
                source: GLOBAL_QUEUE
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
    """),
    )


def test_can_merge_workflows() -> None:
    """Check whether we can merge workflows."""
    my_param = param("num", typ=int)
    value = to_int(num=increment(num=my_param))
    result = sum(left=value, right=increment(num=value))
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw, allow_complex_types=True)
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
              float
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
    wkflw = construct(result, simplify_ids=True)
    subworkflows = render(wkflw)
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
            default: 3
            label: CONSTANT
            type: int
          num:
            label: num
            type: int
        outputs:
          out:
            label: out
            outputSource: increment-1/out
            type: int
        steps:
          increment-1:
             in:
               num:
                 source: add_constants-1/out
             out: [out]
             run: increment
          increment-2:
             in:
               num:
                 source: num
             out: [out]
             run: increment
          add_constants-1:
             in:
               CONSTANT:
                 source: CONSTANT
               num:
                 source: increment-2/out
             out: [out]
             run: add_constants
    """)

    assert osubworkflows[0] == (
        "add_constants-1",
        yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          num:
            label: num
            type: int
          CONSTANT:
            default: 3
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
    """),
    )


def test_combining_attrs_and_factories() -> None:
    """Check combining attributes from a dataclass with factory-produced instances."""
    Pack = factory(PackResult)

    @task()
    def sum(left: int, right: int) -> int:
        return left + right

    @workflow()
    def black_total(pack: PackResult) -> int:
        return sum(left=pack.spades, right=pack.clubs)

    pack = Pack(hearts=13, spades=13, diamonds=13, clubs=13)
    wkflw = construct(black_total(pack=pack), simplify_ids=True)
    cwl = render(wkflw, allow_complex_types=True, factories_as_params=True)
    assert cwl["__root__"] == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          PackResult-1:
            label: PackResult-1
            type: record
        outputs:
          out:
            label: out
            outputSource: black_total-1/out
            type: int
        steps:
          black_total-1:
            in:
              pack:
                source: PackResult-1/out
            out:
            - out
            run: black_total
    """)

    assert cwl["black_total-1"] == yaml.safe_load("""
        class: Workflow
        cwlVersion: 1.2
        inputs:
          pack:
            label: pack
            type: record
        outputs:
          out:
            label: out
            outputSource: sum-1-1/out
            type: int
        steps:
          sum-1-1:
            in:
              left:
                source: pack/spades
              right:
                source: pack/clubs
            out:
            - out
            run: sum
    """)
