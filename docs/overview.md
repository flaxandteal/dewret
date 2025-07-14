# What is Dewret and Why?

## Executive Summary

**Dewret** (DEclarative Workflow REndering Tool) is a Python framework that bridges the gap between dynamic, code-based workflows and static, portable workflow specifications. It enables organizations to write complex computational workflows in familiar Python syntax while automatically generating standard workflow formats (CWL) for execution across diverse computing environments.

## The Problem

Organizations running computational workloads face a fundamental trade-off:

- **Dynamic workflows** (Python/Dask) are flexible and developer-friendly but lack portability, reproducibility, and optimization opportunities
- **Static workflows** (CWL/Snakemake/etc.) are portable and optimizable but require verbose YAML/DSL syntax with steep learning curves

This forces teams to choose between developer productivity and operational requirements, often resulting in:

- Vendor lock-in to specific execution platforms
- Difficulty sharing workflows across teams and institutions
- Inability to leverage workflow-specific optimizations
- Challenges with reproducibility and version control

## The Dewret Solution

Dewret eliminates this trade-off by providing a **Python-first authoring experience** that automatically renders to **portable, standards-compliant workflows**.

### Core Value Proposition

```python
# Write natural Python code
@task()
def process_data(file: str) -> pd.DataFrame:
    return pd.read_csv(file)

@workflow()
def analysis_pipeline(data_file: str):
    df = process_data(data_file)
    return analyze(df)
```

↓ **Renders automatically to** ↓

```yaml
# Industry-standard CWL workflow
class: Workflow
cwlVersion: v1.2
steps:
  process_data:
    run: process_data.cwl
    in: {file: data_file}
# ... complete executable workflow
```

## Key Benefits

### **For Development Teams**

- **Familiar Syntax**: Write workflows in Python using existing skills and tools
- **Rich Type System**: Leverage Python's type hints for automatic workflow validation
- **Debugging Support**: Test and debug workflows locally before deployment
- **Code Reuse**: Integrate with existing Python libraries and frameworks
- **IDE Support**: Type-hinting and eager execution simplify step debugging in IDEs

### **For Operations Teams**

- **Multi-Platform Execution**: Deploy to multiple environment types
- **Performance Optimization**: Enable workflow engines to optimize execution plans
- **Container Support**: Automatic integration with containerized environments
- **Resource Management**: Leverage workflow engine capabilities for scheduling and scaling

### **For Organizations**

- **Vendor Independence**: Avoid lock-in to specific workflow platforms
- **Standards Compliance**: Generate workflows compatible with chosen industry standards
- **Cost Optimization**: Test locally, optimize workflows before expensive HPC runs
- **Audit Trail**: Git-versionable static workflows enhance reproducibility and issue bisection

## Technical Architecture

### **Lazy Evaluation Engine**
Built on Dask's proven lazy evaluation system, Dewret constructs workflow graphs without executing code, enabling analysis and optimization before rendering.

### **Multi-Renderer Support**
- **CWL**: Common Workflow Language for maximum portability
- **Extensible**: Plugin architecture for custom renderers

### **Enterprise Features**
- **Early Error Detection**: Catch workflow issues before resource-intensive execution
- **Static Analysis**: Enable workflow linting, security scanning, and optimization
- **CI/CD Integration**: Automated workflow testing and validation
- **Documentation Generation**: Automatic workflow documentation from code

## Use Cases

### **Scientific Computing**
- Multi-step data analysis pipelines
- High-performance computing workloads
- Cross-institutional collaboration

### **Data Engineering**
- ETL/ELT pipeline development
- Multi-cloud data processing
- Batch processing workflows

### **Machine Learning**
- Model training pipelines
- Feature engineering workflows
- MLOps automation

## Advanced Usage

Proof-of-concept work that can be extended to full features:

- **Graph-based Checkpointing**: By knowing output goals and any memoization achieved in previous execution,
  with a defined "audit log" schema, dewret will eliminate redundant computation consistently,
  before reaching the executor, even for complex DAGs
- **Multi-engine Workflows**: From a single workflow in dewret, coarse- and fine-grained execution can be
  achieved by making the "split-points" between workflows on (e.g.) Argo and workflows on (e.g.) dask workers
  choosable at render-time.

## Why Dewret Now?

### **Industry Trends**
- Growing adoption of workflow standards (CWL adoption up 300% in life sciences)
- Increased focus on reproducible research and FAIR data principles
- Multi-cloud strategies requiring portable workloads

### **Competitive Advantage**
- **Unique Position**: Only tool providing Python-native authoring with multi-format rendering
- **Proven Foundation**: Built on mature technologies (Dask, established workflow standards)
- **Active Ecosystem**: Integration with existing Python scientific computing stack

### **Return on Investment Drivers**
- **Reduced Development Time**: Far faster workflow development vs. native YAML
- **Improved Reliability**: Early error detection prevents costly HPC failures
- **Enhanced Portability**: Single codebase runs across multiple execution environments
- **Team Efficiency**: Leverage existing Python expertise instead of learning new DSLs

## Getting Started

Dewret is available as an open-source Python package with comprehensive documentation, examples, and community support. Organizations can evaluate Dewret with existing workflows and see immediate benefits in developer productivity and workflow portability.

**Installation**: `pip install dewret`  
**Documentation**: https://flaxandteal.github.io/dewret  
**Repository**: https://github.com/flaxandteal/dewret
