# Dewret CWL Example

This example demonstrates the complete Dewret workflow: writing a task in Python, rendering it to CWL format, and executing it with cwltool.

## Files

- **`workflow.py`** - Original Python workflow definition using Dewret decorators
- **`workflow.cwl`** - CWL workflow rendered from the Python code
- **`my_task`** - CWL CommandLineTool definition for the Python task
- **`input.yml`** - Custom input configuration file

## Prerequisites

Install the required dependencies:

```bash
pip install dewret cwltool
```

## Running the Example

### Step 1: Generate CWL from Python (Optional)
The CWL files are already generated, but you can regenerate `workflow.cwl` with:

```bash
python -m dewret workflow.py my_task input:'"value"' --pretty > workflow.cwl
```

### Step 2: Execute with cwltool

#### Basic execution (uses default input "value"):
```bash
python -m cwltool workflow.cwl
```

**Expected output:**
```json
{
    "out": "Processed: value"
}
```

#### Custom input execution:
```bash
python -m cwltool workflow.cwl input.yml
```

**Expected output:**
```json
{
    "out": "Processed: custom_input"
}
```

## Understanding the Workflow

1. **Python Definition** (`workflow.py`):
   - Uses `@task()` decorator from Dewret
   - Defines a simple function that processes input strings
   - Type hints provide CWL type information
   - Currently, you must manually create `my_task` as a file, the CommandLineTool that embeds the Python function

2. **CWL Rendering** (`workflow.cwl`):
   - Running dewret outputs a workflow CWL file
   - `workflow.cwl`: Main workflow with inputs, outputs, and steps
   - This is deterministic and git-committable

3. **Execution**:
   - cwltool reads the CWL workflow
   - Executes the embedded Python code
   - Returns structured output

## Customizing Input

Edit `input.yml` to change the input value:

```yaml
my_task-1-input: "your_custom_value"
```

Then run:
```bash
python -m cwltool workflow.cwl input.yml
```

## Key Benefits Demonstrated

- **Python-first**: Write natural Python code with familiar syntax
- **Static rendering**: Generate portable CWL workflows
- **Tool interoperability**: Execute with standard CWL tools
- **Type safety**: Automatic type conversion from Python to CWL
- **Reproducibility**: Static workflows can be version controlled and shared
