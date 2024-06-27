# Glossary

#### Construct
- An alias pointing to an instance of the TaskManager class, used for constructing a set of [tasks](#task) into a dewret workflow instance.

#### Nested Task
- Specific type of [task](#task) designed to encapsulate multiple tasks. Nested tasks are the culmination (or result) of multiple tasks represented as a single task in a dewret [workflow](#workflow).

#### Render
- Render is a renderer-specific method that generates a workflow structure in the specific workflow language. Current defaults are CWL and Snakemake. 

#### Step
- A step in a dewret workflow represents a single unit of work. It contains a single task and the arguments for that task. Corresponds to a CWL [Step](https://www.commonwl.org/user_guide/topics/workflows.html#workflows) or a Snakemake [Rule](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html)

#### Task
- A task is the function scheduled to be executed later. Corresponds to a CWL [Process](https://www.commonwl.org/user_guide/introduction/basic-concepts.html#processes-and-requirements)

#### Workflow
- A workflow is designed to define, manage, and execute a series of tasks (or steps) that make use of both local and global parameters.
