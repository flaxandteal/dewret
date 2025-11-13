# Dewret: Declarative Workflow Rendering Tool

*Write workflows in Python, render to any format, execute anywhere*

---

## The Problem

**Dynamic workflows** (Python/Dask): Flexible but not portable  
**Static workflows** (CWL/Snakemake): Portable but verbose

**Solution**: Write once in Python, render to any workflow language

```python
@task()
def process_data(file: str) -> pd.DataFrame:
    return pd.read_csv(file)

@workflow()
def analysis_pipeline(data_file: str):
    df = process_data(data_file)
    return analyze(df)
```

→ Renders to CWL, Snakemake, or custom formats

---

## Core Design & Architecture

<div class="mermaid">
graph LR
    A[Python Code] --> B[Lazy Evaluation<br/>(Dask Backend)]
    B --> C[Workflow Graph]
    C --> D[Renderer]
    D --> E[CWL/Snakemake/<br/>Custom Format]
</div>

**Key Principles:**
- **Lazy evaluation** for workflow construction
- **Separation of concerns** (logic vs. execution)
- **Python-first** (no new DSLs)

---

## Key Features

### Complex Data Structures & Nested Workflows

```python
@dataclass
class Results:
    accuracy: float
    model: str

@workflow()
def preprocessing(raw: str) -> pd.DataFrame:
    return clean_data

@workflow()
def ml_pipeline(data_path: str):
    clean = preprocessing(data_path)
    return train_model(clean)
```

### Advanced Capabilities
- **Eager evaluation** for debugging and IDE: `dewret run --eager`
- **Field access**: `result.accuracy`
- **Automatic deduplication** of identical tasks via dask graph-pruning

---

## Real-World Impact

### Benefits
- **Early error detection** before expensive HPC runs
- **True reproducibility** with git-versioned workflows
- **Easy uploading** from local testing
- **Fully mypy-compatible** for comprehensive type-checking

### Use Cases
```python
# Scientific Computing
@workflow()
def finite_element_pipeline(settings) -> SolverResult:
    mesh: Mesh = build_mesh(...)
    return solver.solve(...)

# Machine Learning
@workflow()
def ml_experiment(**config: Unpack[MLConfig]) -> list[float]:
    features= engineer_features(generate_data_workflow())
    model = train_model(features, config)
    return model.evaluate()
```

---

## Getting Started

### Quick Start
```bash
pip install dewret
```

```python
# workflow.py
from dewret.tasks import task

@task()
def my_task(input: str) -> str:
    return f"Processed: {input}"
```

```bash
python -m dewret workflow.py my_task input:'"value"' --pretty > workflow.cwl
# Create a my_tool CWL tool to run Python
python -m cwltool workflow.cwl
```

### Summary
**Write** in Python → **Debug** locally → **Render** to any format → **Execute** anywhere
