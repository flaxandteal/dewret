"""Add metadata to steps."""

import yaml
from dewret.tasks import construct
from dewret.renderers.cwl import render
from dewret.utils import hasher, meta

from ._lib.extra import (
    pi,
)


def test_adding_basic_metadata() -> None:
    """Check whether we can produce simple CWL.

    Produces simplest possible CWL from a workflow, using
    a pure function.
    """
    result = pi()
    meta(result).labels = ["label-1"]
    workflow = construct(result)
    rendered = render(workflow)["__root__"]
    hsh = hasher(("pi",))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs:
          out:
            label: out
            outputSource: pi-{hsh}/out
            type: float
        steps:
          pi-{hsh}:
            run: pi
            in: {{}}
            out: [out]
            labels: ["label-1"]
    """)


def test_adding_clashing_metadata_keys() -> None:
    """Check that adding metadata for CWL keys is ignored."""
    result = pi()
    meta(result).labels = ["label-1"]
    meta(result).out = ["not out"]
    workflow = construct(result)
    rendered = render(workflow)["__root__"]
    hsh = hasher(("pi",))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs:
          out:
            label: out
            outputSource: pi-{hsh}/out
            type: float
        steps:
          pi-{hsh}:
            run: pi
            in: {{}}
            out: [out]
            labels: ["label-1"]
    """)


def test_metadata_as_items() -> None:
    """Check that adding metadata for CWL keys is ignored."""
    result = pi()
    meta(result)["labels"] = ["label-1"]
    workflow = construct(result)
    rendered = render(workflow)["__root__"]
    hsh = hasher(("pi",))

    assert rendered == yaml.safe_load(f"""
        cwlVersion: 1.2
        class: Workflow
        inputs: {{}}
        outputs:
          out:
            label: out
            outputSource: pi-{hsh}/out
            type: float
        steps:
          pi-{hsh}:
            run: pi
            in: {{}}
            out: [out]
            labels: ["label-1"]
    """)
