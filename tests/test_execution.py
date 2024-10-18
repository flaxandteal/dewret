"""Check eager behaviour and execution to evaluate a task/workflow."""

import math
from sympy import Expr

from dewret.tasks import (
    workflow,
    factory,
    task,
    evaluate,
)
from dewret.core import set_configuration
from ._lib.extra import (
    pi,
    PackResult
)

def test_basic_eager_execution() -> None:
    """Check whether we can run a simple flow without lazy-evaluation.

    Will skip dask delayeds and execute during construction.
    """
    result = pi()

    # Execute this step immediately.
    output = evaluate(result, execute=True)
    assert output == math.pi

    with set_configuration(eager=True):
        output = pi()

    assert output == math.pi

def test_eager_execution_of_a_workflow() -> None:
    """Check whether we can run a workflow without lazy-evaluation.

    Will skip dask delayeds and execute during construction.
    """
    @workflow()
    def pair_pi() -> tuple[float, float]:
        return pi(), pi()

    # Execute this step immediately.
    with set_configuration(flatten_all_nested=True):
        result = pair_pi()
        output = evaluate(result, execute=True)

    assert output == (math.pi, math.pi)

    with set_configuration(eager=True):
        output = pair_pi()

    assert output == (math.pi, math.pi)


def test_eager_execution_of_a_rich_workflow() -> None:
    """Ensures that a workflow with both tasks and workflows can be eager-evaluated."""
    Pack = factory(PackResult)

    @task()
    def sum(left: int, right: int) -> int:
        return left + right

    @workflow()
    def black_total(pack: PackResult) -> int:
        return sum(left=pack.spades, right=pack.clubs)

    pack = Pack(hearts=13, spades=13, diamonds=13, clubs=13)

    output_sympy: Expr = evaluate(black_total(pack=pack), execute=True)
    clubs, spades = output_sympy.free_symbols
    output: int = output_sympy.subs({spades: 13, clubs: 13})

    assert output == 26

    with set_configuration(eager=True):
        pack = Pack(hearts=13, spades=13, diamonds=13, clubs=13)
        output = black_total(pack=pack)

    assert output == 26
