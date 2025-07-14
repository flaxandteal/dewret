# Dewret: Declarative Workflow Rendering Tool

*Bridging Dynamic Python and Static Workflows*

---

## The Challenge: Writing Workflows Today

- **Dynamic Approach** (Dask, Python)
  - ✅ Flexible and expressive
  - ✅ Familiar programming model
  - ❌ Not portable across execution systems
  - ❌ Hard to version and reproduce

- **Static Approach** (CWL, Snakemake)
  - ✅ Portable and reproducible
  - ✅ Optimizable by workflow engines
  - ✅ Generic checkpointing easier
  - ❌ Verbose and restrictive
  - ❌ Steep learning curve

---

## Introduction

### Write Once, Render Anywhere

```python
@task()
def process_data(file: str) -> pd.DataFrame:
    return pd.read_csv(file)

@workflow()
def analysis_pipeline(data_file: str):
    df = process_data(data_file)
    return analyze(df)
```

Renders to CWL, Snakemake, or custom formats

---

## Core Design Principles

### Lazy Evaluation
Built on Dask's delayed execution to construct workflow graphs before rendering

### Separation of Concerns
- Workflow logic vs. execution environment
- One codebase, multiple targets
- Heterogeneous containers

### Python-First Approach
- No new DSLs to learn
- Use your existing code

---

## Architecture Overview

<div class="mermaid">
graph TD
    A[Python Code] --> B[Task Decorators]
    B --> C[Lazy Evaluation]
    C --> D[Workflow Graph]
    D --> E[Renderer]
    E --> F[CWL]
    E --> G[Snakemake]
    E --> H[Custom Format]
</div>


---

## Comparison with Other Tools

### Dewret vs. Dask

| Aspect | Dask | Dewret |
|--------|------|--------|
| **Focus** | Parallel execution | Workflow rendering |
| **Output** | Computed results | Static workflows |
| **Use Case** | Data processing | Workflow portability |

**Note**: Dewret uses Dask as its lazy evaluation backend, enabling execution in Dask environments.

--

### Dewret vs. Kubeflow

| Aspect | Kubeflow | Dewret |
|--------|----------|--------|
| **Scope** | Full ML platform | Workflow definition |
| **Target** | Kubernetes | Any workflow engine |
| **Complexity** | Enterprise-scale | Lightweight |

**Note**: Dewret can render workflows for execution on Kubeflow.

---

## Key Features

### Complex Data Structures

```python
@dataclass
class Results:
    accuracy: float
    model: str
    metrics: dict

@task()
def train_model(data: pd.DataFrame) -> Results:
    # Training logic here
    return Results(accuracy=0.95, 
                   model="rf", 
                   metrics={...})
```

--

### Nested Workflows

```python
@workflow()
def preprocessing(raw_data: str) -> pd.DataFrame:
    # Preprocessing steps
    return cleaned_data

@workflow()
def main_pipeline(data_path: str):
    clean_data = preprocessing(data_path)
    results = analyze(clean_data)
    return results
```

--

### Eager Evaluation Mode

```bash
# Debug your workflow interactively
dewret run pipeline.py --eager

# Render to static format
dewret render pipeline.py --format cwl
```

Ideal for development and debugging workflows before rendering.

---

## Real-World Benefits

### Early Error Detection
- Catch workflow bugs before HPC time
- Validate DAG structure immediately

### True Reproducibility
- Git-version your workflows
- Exact same execution across time

### Cost Efficiency
- Test without cluster resources
- Optimize before deployment


## Use Cases

### Scientific Computing

```python
@workflow()
def finite_element_pipeline(settings: FESettingsDataclass):
    m = build_mesh(...)
    a, L = assemble_meshfree_matrices(...)
    solver = construct_solver(settings)
    result = solver.solve(a, L, m)
    return result
```

--

### Machine Learning
```python
@workflow()
def ml_experiment(config: dict):
    data = load_dataset(config["dataset"])
    features = engineer_features(data)
    model = train_model(features, config["params"])
    return evaluate(model)
```

--

### Data Processing
```python
@workflow()
def etl_pipeline(source: str):
    raw = extract(source)
    transformed = transform(raw)
    load(transformed, destination)
    return transformed
```

---

## Getting Started

```bash
pip install dewret
```

---

## Advanced Features

### Factory Functions
```python
@task()
def create_model(config: dict) -> Model:
    return Model(**config)

# Handles complex initialization
model = create_model({"layers": [64, 32], 
                     "dropout": 0.2})
```

--

### Field Access
```python
@task()
def process() -> Results:
    return Results(score=0.9, data=[1,2,3])

# Access specific fields
score = process().score
data = process().data
```

--

### Automatic Deduplication
```python
# These create only one task node
result1 = expensive_computation(data)
result2 = expensive_computation(data)  # Same
```


## Roadmap

### Current Features
- CWL & Snakemake renderers
- Dask backend integration
- Complex data structure support

### Future Development
- Additional renderers (Nextflow, WDL)
- Alternative backends
- Workflow optimization passes
- Visual workflow editor

---

## Summary

**Write** workflows in Python → **Debug** with eager evaluation → **Render** to any format → **Execute** anywhere
