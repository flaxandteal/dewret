"""Demo: import a render-time class and attempt to render a workflow.

It attempts to emulate the way method functions of basic types like str.upper() work,
but importing a custom class doesn't work

The error printed gives us the clue:

> dewret.tasks.TaskException: Non-references must be a serializable type: prefix><test_import.MyStr object at 0x0000016E55BF0590> <class 'test_import.MyStr'>
> Task create_greeting_in_all_caps declared in <module>
"""

from dewret.renderers.cwl import render
from dewret.tasks import task, construct, workflow
from dewret.annotations import AtRender

# Run demo only when executed as a script (prevents pytest collection from executing it)
if __name__ == "__main__":
    # import the class to annotate inputs
    from render_time_module import MyStr  # type: ignore[import-not-found]

    # need at least one task to render
    @task()
    def concat_strings(str1: str, str2: str) -> str:
        """Concatenate two strings.

        This simple task is used by the demo workflow. It's intentionally
        trivial: it just returns the concatenation of ``str1`` and ``str2``.
        """
        return str1 + str2

    @workflow()
    def create_greeting_in_all_caps(
        prefix: AtRender[MyStr], name: AtRender[MyStr]
    ) -> str:
        """Create a greeting by uppercasing prefix and name at render time.

        The inputs ``prefix`` and ``name`` are annotated with
        ``AtRender[MyStr]``, which means they are available during render-time
        processing. Methods called on those objects (for example ``.upper()``) are
        executed while the workflow is being rendered rather than at runtime.
        """
        prefix_cap = prefix.upper()
        name_cap = name.upper()
        return concat_strings(str1=prefix_cap, str2=name_cap)

    result = create_greeting_in_all_caps(prefix=MyStr("Hello to "), name=MyStr("John"))

    wkflw = construct(result, simplify_ids=True)
    cwl = render(wkflw)

    print(cwl)
