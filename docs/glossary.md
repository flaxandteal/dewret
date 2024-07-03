# Glossary

#### Construct
- To `construct` a workflow in dewret is to pull the connected steps into a single structure.

#### Sub Workflow
- A subworkflow is a nested or hierarchical workflow. It is a workflow defined within another workflow, allowing for the encapsulation and reuse of complex operations as a single, higher-level step in the parent workflow.

- Specific type of [task](#task) designed to encapsulate multiple tasks. Nested tasks are the culmination (or result) of multiple tasks represented as a single task in a dewret [workflow](#workflow).

#### Render
- To render a workflow is to generate an executable workflow in a specific workflow language such as CWL and Snakemake. 

#### Step
- A step in a dewret workflow represents a single unit of work. It contains a single task and the arguments for that task. Corresponds to a CWL [Step](https://www.commonwl.org/user_guide/topics/workflows.html#workflows) or a Snakemake [Rule](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html)

#### Task
- A task is the function scheduled to be executed later. Corresponds to a CWL [Process](https://www.commonwl.org/user_guide/introduction/basic-concepts.html#processes-and-requirements)

#### Workflow
- A workflow is designed to define, manage, and execute a series of tasks that make use of both local and global parameters.
