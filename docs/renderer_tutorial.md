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
        output_file="data/processed_data.txt"
    run:
        with open(output.output_file, "w") as f:
            f.write("data")

        return output_file
```

## 2. Create WorkflowDefinition.

The WorkflowDefinition class is responsible for transforming each step from a constructed `dewret` workflow into an executable step in the target workflow language (e.g. a Snakemake rule). This class should encapsulate workflow-level information, such as the list of steps to be executed, and any workflow-scope input/ouput. It should also contain a class method that initializes the `WorkflowDefinition` from an `dewret` `Workflow` (such as `from_workflow` below), and a method that renders the workflow as a Python dict (as in the `render` method below).

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

## 3.  Ensuring Our Module is Recognized as a Render Module
To have our custom renderer identified by Dewret as a valid renderer, we need to implement the `BaseRenderModule` along with one of the two protocols: `RawRenderModule` or `StructuredRenderModule`.

#### Implementing BaseRenderModule
The `BaseRenderModule` defines the foundation for a custom renderer. To implement this protocol, we need to define the `default_config()` method, which provides default configurations for our renderer.

```python 
def default_config() -> CWLRendererConfiguration:
    """Default configuration for this renderer.

    This is a hook-like call to give a configuration dict that this renderer
    will respect, and sets any necessary default values.

    Returns: a dict with (preferably) raw type structures to enable easy setting
        from YAML/JSON.
    """
    return {
        "allow_complex_types": False,
        "factories_as_params": False,
    }
```

After implementing `BaseRenderModule`, you need to implement either the `RawRenderModule` or `StructuredRenderModule` protocol, depending on how you want to handle the workflow rendering.

#### Implementing either RawRenderModule or StructuredRenderModule
The `StructuredRenderModule` is designed for structured workflows that are directly ready to be output in the respective format (e.g., CWL, Snakemake, etc.). The key method to implement is `render`, which converts a workflow into a structured, serializable format.
```python
def render(
        self, workflow: WorkflowProtocol, **kwargs: RenderConfiguration
    ) -> dict[str, dict[str, RawType]]:
        """Turn a workflow into a serializable structure.

        Returns: one or more subworkflows with a `__root__` key representing the outermost workflow, at least.
        """
        ...
```
In this method:
- You receive a workflow and potentially some optional configurations.
- You return a dictionary where the `__root__` key holds the primary workflow and any additional subworkflows are nested inside the returned structure.

If you prefer more flexibility and want the structuring to be handled by the user, you can implement the `RawRenderModule` protocol. This requires defining the `render_raw` method, which converts a workflow into raw, flat strings.
```python
    def render_raw(
        self, workflow: WorkflowProtocol, **kwargs: RenderConfiguration
    ) -> dict[str, str]:
        """Turn a workflow into flat strings.

        Returns: one or more subworkflows with a `__root__` key representing the outermost workflow, at least.
        """
        ...
```
In this method:

- The workflow is rendered as raw, unstructured strings.
- The user is responsible for handling the structuring of the rendered output.

## 4. Create a StepDefinition.

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
    """Represents a Snakemake-renderable step definition in a dewret workflow.

    Attributes:
        name (str): The name of the step.
        run (str): The run block definition for the step.
        params (List[str]): The parameter definitions for the step.
        output (list[str]: The output definition for the step.
        input (List[str]): The input definitions for the step.

    Methods:
        from_step(cls, step: Step) -> "StepDefinition": Constructs a StepDefinition
            object from a Step object, extracting step information and components
            from the step and converting them to Snakemake format.
        render(self) -> dict[str, MainTypes]: Renders the step definition as a dictionary
            suitable for use in Snakemake workflows.
    """

    # You can consider each step as a separate rule.
    # Each field in this class represents a separate block in the rule definition
    name: str # name of the rule
    input: list[str] # Input block
    params: list[str] # Params block
    output: list[str] # Output block
    run: list[str] # Run block - where the instructions for the task are

    @classmethod
    def from_step(cls, step: Step) -> "StepDefinition":
        """Constructs a StepDefinition object from a Step.

        Args:
            step (Step): The Step object from which step information and components
                are extracted.

        Returns:
            StepDefinition: A StepDefinition object containing the converted step
                information and components.
        """
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

    def render(self) -> dict[str, MainTypes]:
        """Renders the step definition as a dictionary.

        Returns:
            dict[str, MainTypes]: A dictionary containing the components of the step
                definition, for use in Snakemake workflows.
        """
        return {
            "run": self.run,
            "input": self.input,
            "params": self.params,
            "output": self.output,
        }
```

## 5. Create the Separate block definitions.

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
    """Represents input and parameter definitions block for a Snakemake-renderable workflow step.

    Attributes:
        inputs (List[str]): A list of input definitions.
        params (List[str]): A list of parameter definitions.

    Methods:
        from_step(cls, step: Step) -> "InputDefinition": Constructs an InputDefinition
            object from a Step object, extracting inputs and parameters and converting
            them to Snakemake-compatible format.
        render(self) -> dict[str, str]: Renders the input and parameter definitions
            as a dictionary for use in Snakemake Input and Params blocks.
    """

    # As we already mention input and params block have similar generation
    # So it made sence to encapsulate them into one Definition
    inputs: list[str]
    params: list[str]

    @classmethod
    def from_step(cls, step: Step) -> "InputDefinition":
        """Constructs an InputDefinition object from a Step.

        Args:
            step (Step): The Step object from which input and parameter block definitions are
                extracted.

        Returns:
            InputDefinition: An InputDefinition object.
        """
        params = []
        inputs = []
        # The keys represent the names of the arguments of the @tasks in our snakemake_workflow.py.
        # The params represent the values.
        for key, param in step.arguments.items():
            # We check if the param is a reference.
            # If it is then it's an input requirement for the rule to run, so we put it in the input block
            if isinstance(param, Reference):
                ref = ReferenceDefinition.from_reference(param).render().replace("-","_").replace("/out", ".output")
                input = f"{key}=rules.{ref}.output_file"
                inputs.append(input)
                params.append(input + ",")
            # If it's not - we put it in the params block for use in the RunDefinition
            elif isinstance(param, Raw):
                customized = f"{key}={to_snakemake_type(param)},"
                params.append(customized)

        # Since the params must be comma separated except the last one - we remove the last comma
        if params:
            params[len(params) - 1] = params[len(params) - 1].replace(",", "")

        return cls(inputs=inputs, params=params)

    def render(self) -> dict[str, list[str]]:
        """Renders the input and parameter definitions as a dictionary.

        Returns:
            dict[str, list[MainTypes]]: A dictionary containing the input and parameter definitions,
                for use in Snakemake Input and Params blocks.
        """
        return {"inputs": self.inputs, "params": self.params}

```

#### RunDefinition:

The RunDefinition class is responsible for rendering the run block, which contains the actual instructions for the task.

```python
@define
class RunDefinition:
    # This is where we handle the execution of the task itself.
    """Represents a Snakemake-renderable run block for a dewret workflow step.

    Attributes:
        method_name (str): The name of the method to be executed in the snakefile run block.
        rel_import (str): The relative import path of the method.
        args (List[str]): The arguments to be passed to the method.

    Methods:
        from_task(cls, task: Task) -> "RunDefinition": Constructs a RunDefinition
            object from a Task object, extracting method information and arguments
            from the task and converting them to Snakemake-compatible format.

        render(self) -> list[str]: A list containing the import statement and the method
            call statement, for use in Snakemake run block.
    """

    method_name: str
    rel_import: str
    args: list[str]

    @classmethod
    def from_task(cls, task: Task) -> "RunDefinition":
        """Constructs a RunDefinition object from a Task.

        Args:
            task (Task): The Task object from which method information and arguments
                are extracted.

        Returns:
            RunDefinition: A RunDefinition object containing the converted method
                information and arguments.
        """
        # Since we can import our snakemake_workflow.py @tasks we need the relative path
        relative_path = get_method_rel_path(task.target)
        # If we need to make any customization to the import
        rel_import = f"{relative_path}"

        args = get_method_args(task.target)
        signature = [
            f"{param_name}=params.{param_name}"
            for param_name in args.parameters.keys()
        ]

        return cls(method_name=task.name, rel_import=rel_import, args=signature)

    def render(self) -> list[str]:
        """Renders the run block as a list of strings.

        Returns:
            list[str]: A list containing the import statement and the method
                call statement, for use in Snakemake run block.
        """
        signature = ", ".join(f"{arg}" for arg in self.args)
        # The comma after the last element is mandatory for the structure of rule onces it's used in yaml.dump
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
    """Represents the output definition block for a Snakemake-renderable workflow step.

    Attributes:
        output_file (str): The output file definition.

    Methods:
        from_step(cls, step: Step) -> "OutputDefinition": Constructs an OutputDefinition
            object from a Step object, extracting and converting the output file definition
            to Snakemake-compatible format.

        render(self) -> list[str]: Renders the output definition as a list
            suitable for use in Snakemake Output block.
    """

    output_file: str

    @classmethod
    def from_step(cls, step: Step) -> "OutputDefinition":
        """Constructs an OutputDefinition object from a Step.

        Args:
            step (Step): The Step object from which the output file definition is extracted.

        Returns:
            OutputDefinition: An OutputDefinition object, for use in Snakemake Output block.
        """
        # Since snakemake commonly communicates using files.
        # Output file must always be called - `output_file`
        # Further code could be added to handled if it's a reference in case we want take care of multiple tasks writing to the same output file.
        output_file = step.arguments["output_file"]
        if isinstance(output_file, Raw):
            args = to_snakemake_type(output_file)

        return cls(output_file=args)

    def render(self) -> list[str]:
        """Renders the output definition as a list.

        Returns:
            list[str]: A list containing the output file definition, for use in a Snakemake Output block.
        """
        # The comma after the last element is mandatory for the structure of rule onces it's used in yaml.dump
        # It adds the new line to the output block
        return [
            f"output_file={self.output_file}",
        ]
```

Integrate these block definitions into the StepDefinition class as demonstrated in [Step 3](#3-create-a-stepsdefinition). Each StepDefinition will use these block definitions to render the complete step in the target workflow language.

## 6. Helper methods.

In this step, you'll define helper methods that will assist you in converting workflow components into the target workflow language format. In our case these methods will handle type conversion, extracting method arguments, and computing relative paths.

### Example:
We'll define the following helper methods for our Snakemake renderer:

1. to_snakemake_type(param: Raw) -> str: Converts a raw type to a Snakemake-compatible Python type.
2. get_method_args(func: Lazy) -> inspect.Signature: Retrieves the argument names and types of a lazy-evaluatable function.
3. get_method_rel_path(func: Lazy) -> str: Computes the relative path of the module containing the given function.

#### Type Conversion Helper:

```python
# Basic types returned from dewret will look like this "str|valueOfParam".
# We'll need to convert them.
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
# We need to get the signature of the method. 
def get_method_args(func: Lazy) -> inspect.Signature:
    args = inspect.signature(func)
    return args

```

#### Relative Path Computation Helper:

```python
# Computes the relative path
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
from dewret.utils import Raw, BasicType
from dewret.workflow import Lazy
from dewret.workflow import Reference, Workflow, Step, Task

RawType = BasicType | list[str] | list["RawType"] | dict[str, "RawType"]
```

## To run this example: 

1. Import the snakemake renderer into your @tasks file
2. There's an example in [snakemake_tasks.py](../example/snakemake_renderer/basic_example/snakemake_tasks.py)
3. Run it:
```shell
python snakemake_tasks.py
```
