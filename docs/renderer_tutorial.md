# Step-by-Step Guide to Writing a Custom Renderer

## 1. Understand the Target Workflow Language


Before writing any code, it is essential to fully understand the target workflow language. This includes syntax, structure, and specific requirements. By breaking down each key `dewret.workflow` task into smaller components, you can better map your workflow definitions to the target language.

### Example: 

In Snakemake, a workflow task is generally created by:
1. Defining the task. - `rule process_data`
2. Defining the input required for the rule to run(dependencies). - `input: "data/raw_data.txt"`
3. Defining the output required for the rule to be considered finished. - `output: "data/processed_data.txt"`
4. Defining the actual work that the task will do. - in this case:`shell: ...`

```bash
rule process_data: # Example Snakemake rule/task
    input:
        "data/raw_data.txt"
    output:
        "data/processed_data.txt"
    shell:
        """
        tr '[:lower:]' '[:upper:]' < {input} > {output}
        """
```

## 2. Create WorkflowDefinition.

The WorkflowDefinition class is responsible for transforming each step from a constructed dewret.workflow object into an executable rule in the target workflow language (e.g., Snakemake). Steps are the `@tasks` you have defined that you want to convert into executable workflow language steps.

### Example:
```python
@define
class WorkflowDefinition:
    steps: list[StepDefinition]

    # Returns a WorkflowDefinition instanace.
    # Steps contains all of the tasks you want to convert to the target WL tasks.
    @classmethod
    def from_workflow(cls, workflow: Workflow) -> "WorkflowDefinition":
        return cls(steps=[StepDefinition.from_step(step) for step in workflow.steps])

    # Returns each task as a Snakemake executable rule.
    def render(self) -> dict[str, RawType]:
        return {
            f"rule {step.name.replace("-", "_")}": step.render() for step in self.steps
        }
```

## 3. Create a StepsDefinition.

Create a StepsDefinition class create each of the code blocks needed for a rule(step) to be executable in Snakemake.
When you have defined each block in your target workflow language task from [step 1](#1-understand-the-target-workflow-language),
you can go ahead and create, for each of the code blocks required to run a Snakemake rule, a `BlockDefinition` to handle the rendering of each block.

### Example:

In the Snakemake example, we have created:
1. `InputDefinition` - Handles the input block, which contains what is required for a rule to be executed. It also handles the params block since the code for extracting the input and params blocks is similar.
2. `RunDefinition` - Handles the run block which contains the instructions needed for this specific task. 
3. `OutputDefinition` - Handles the output block which is required for the rule to be considered successfully finished.

```python
@define
class StepDefinition:
    name: str
    run: list[str]
    params: list[str]
    output: list[str]
    input: list[str]

    @classmethod
    def from_step(cls, step: Step) -> "StepDefinition":
        input_block = InputDefinition.from_step(step).render()
        run_block = RunDefinition.from_task(step.task).render()
        output_block = OutputDefinition.from_step(step).render()
        return cls(
            name=step.name,
            run=run_block,
            params=input_block["params"],
            input=input_block["inputs"],
            output=output_block,
        )

    def render(self) -> dict[str, RawType]:
        return {
            "run": self.run,
            "input": self.input,
            "params": self.params,
            "output": self.output,
        }
```

## 4. Create the Separate block definitions.

In this step, you'll define classes to handle the rendering of each code block required for a rule (step) to be executable in the target workflow language. Each of these classes will encapsulate the logic for converting parts of a workflow step into the target language format.

### Example:
For the Snakemake workflow language, we will define:

1. InputDefinition: Handles the input block and parameter block.
2. RunDefinition: Handles the run block.
3. OutputDefinition: Handles the output block.

#### InputDefinition: 

The InputDefinition class is responsible for rendering the inputs and parameters required for a Snakemake rule.

```python
@define
class InputDefinition:
    inputs: list[str]
    params: list[str]

    @classmethod
    def from_step(cls, step: Step) -> "InputDefinition":
        params = []
        inputs = []
        for key, param in step.arguments.items():
            if isinstance(param, Reference):
                ref = ReferenceDefinition.from_reference(param).render()
                input = f"{key}=rules.{ref.replace('-','_').replace('/out', '.output')}.output_file"
                inputs.append(input)
                params.append(input + ",")
            elif isinstance(param, Raw):
                customized = f"{key}={to_Snakemake_type(param)},"
                params.append(customized)

        if params:
            params[len(params) - 1] = params[len(params) - 1].replace(",", "")

        return cls(inputs=inputs, params=params)

    def render(self) -> dict[str, list[str]]:
        return {"inputs": self.inputs, "params": self.params}
```

#### RunDefinition:

The RunDefinition class is responsible for rendering the run block, which contains the actual instructions for the task.

```python
@define
class RunDefinition:
    method_name: str
    rel_import: str
    args: list[str]

    @classmethod
    def from_task(cls, task: Task) -> "RunDefinition":
        relative_path = get_method_rel_path(task.target)
        rel_import = f"{relative_path}"

        args = get_method_args(task.target)
        signature = [
            f"{param_name}=params.{param_name}"
            for param_name, param in args.parameters.items()
        ]

        return cls(method_name=task.name, rel_import=rel_import, args=signature)

    def render(self) -> list[str]:
        signature = ", ".join(f"{arg}" for arg in self.args)
        return [
            f"import {self.rel_import}\n",
            f"{self.rel_import}.{self.method_name}({signature})\n",
        ]
```

#### OutputDefinition: 

The OutputDefinition class is responsible for rendering the output block, which specifies the output files or results that indicate the rule has successfully completed.

```python
@define
class OutputDefinition:
    output_file: str

    @classmethod
    def from_step(cls, step: Step) -> "OutputDefinition":
        output_file = step.arguments["output_file"]
        if isinstance(output_file, Raw):
            args = to_Snakemake_type(output_file)

        return cls(output_file=args)

    def render(self) -> list[str]:
        return [
            f"output_file={self.output_file}",
        ]

```

Integrate these block definitions into the StepDefinition class as demonstrated in [Step 3](#3-create-a-stepsdefinition). Each StepDefinition will use these block definitions to render the complete step in the target workflow language.

## 5. Helper methods.

In this step, you'll define helper methods that will assist you in converting workflow components into the target workflow language format. In our case these methods will handle type conversion, extracting method arguments, and computing relative paths.

### Example:
We'll define the following helper methods for our Snakemake renderer:

1. to_snakemake_type(param: Raw) -> str: Converts a raw type to a Snakemake-compatible Python type.
2. get_method_args(func: Lazy) -> inspect.Signature: Retrieves the argument names and types of a lazy-evaluatable function.
3. get_method_rel_path(func: Lazy) -> str: Computes the relative path of the module containing the given function.

#### Type Conversion Helper:

```python
def to_Snakemake_type(param: Raw) -> str:
    typ = str(param)
    if typ.__contains__("str"):
        return f'"{typ.replace("str|", "")}"'
    elif typ.__contains__("bool"):
        return typ.replace("bool|", "")
    elif typ.__contains__("dict"):
        return typ.replace("dict|", "")
    elif typ.__contains__("list"):
        return typ.replace("list|", "")
    elif typ.__contains__("float"):
        return typ.replace("float|", "")
    elif typ.__contains__("int"):
        return typ.replace("int|", "")
    else:
        raise TypeError(f"Cannot render complex type ({typ})")
```

#### Argument Extraction Helper:

```python
def get_method_args(func: Lazy) -> inspect.Signature:
    args = inspect.signature(func)
    return args

```

#### Relative Path Computation Helper:

```python
def get_method_rel_path(func: Lazy) -> str:
    source_file = inspect.getsourcefile(func)
    if source_file:
        relative_path = os.path.relpath(source_file, start=os.getcwd())
        module_name = os.path.splitext(relative_path)[0].replace(os.path.sep, ".")

    return module_name
```

## Imports and custom types required in the SMK example:

```python
import os
import yaml
import inspect
import typing

from attrs import define
from dewret.workflow import Lazy
from dewret.workflow import Reference, Raw, Workflow, Step, Task
from dewret.utils import BasicType

RawType = typing.Union[BasicType, list[str], list["RawType"], dict[str, "RawType"]]
```

## To run this example: 

1. Import the snakemake renderer into your @tasks file
2. There's an example in [snakemake_tasks.py](../example/snakemake_renderer/basic_example/snakemake_tasks.py)
3. Run it:
```shell
python snakemake_tasks.py
```

### Q: Should I add a brief description of dewret in step 1? Should link dewret types/docs etc here?
### Q: Better explain each component(steps, workflows, tasks) available members? Or just link them?
### Q: More inline comments?
### Q: Better explain helper methods?
### Q: Should we abstract some Definitions?
### Q: Should I write a Snakemake example to the docs.
### A: Get details on how that happens and probably yes.