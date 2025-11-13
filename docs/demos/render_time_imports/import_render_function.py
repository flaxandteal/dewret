"""Demo of render-time imports with @workflow and AtRender annotations.

This module demonstrates how to import and use functions at render time
rather than at task construction time
"""

from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from dewret.annotations import AtRender


@task()
def concat_strings(str1: str, str2: str) -> str:
    """Concatenate two strings."""
    return str1 + str2


@workflow()
def create_greeting_in_all_caps(prefix: AtRender[str], name: AtRender[str]) -> str:
    """Create a greeting with all uppercase letters.

    Imports a render-time module function to capitalize strings,
    then concatenates them into a greeting.
    """
    from render_time_module import caps  # type: ignore[import-not-found]

    prefix_cap = caps(prefix)
    name_cap = caps(name)
    return concat_strings(str1=prefix_cap, str2=name_cap)


# Run demo only when executed as a script (prevents pytest collection from executing it)
if __name__ == "__main__":
    result = create_greeting_in_all_caps(prefix="Hello to ", name="John")

    workflow = construct(result, simplify_ids=True)
    cwl = render(workflow)

    print(cwl)
