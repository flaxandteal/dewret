"""Demo of incorrect render-time imports pattern (failure case).

This module shows an example of what NOT to do: defining a function
at module level that is used at render time. Functions used at render time
must be imported inside the workflow from another module to work correctly.
"""

from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from dewret.annotations import AtRender


# need at least one task to render
@task()
def concat_strings(str1: str, str2: str) -> str:
    """Concatenate two strings."""
    return str1 + str2


# this won't work, functions used at render time must be imported inside the workflow, from another module
def caps(arg: str) -> str:
    """Convert a string to uppercase."""
    return arg.upper()


@workflow()
def create_greeting_in_all_caps(prefix: AtRender[str], name: AtRender[str]) -> str:
    """Create a greeting with all uppercase letters (FAILS at render time).

    This workflow incorrectly uses a module-level function with render-time
    parameters. This will fail during rendering.
    """
    prefix_cap = caps(prefix)
    name_cap = caps(name)
    return concat_strings(str1=prefix_cap, str2=name_cap)


# Run demo only when executed as a script (prevents pytest collection from executing it)
if __name__ == "__main__":
    result = create_greeting_in_all_caps(prefix="Hello to ", name="John")

    wkflw = construct(result, simplify_ids=True)
    cwl = render(wkflw)

    print(cwl)
