# dewret

DEclarative Workflow REndering Tool

_Pron_: durr-it, like "durable"

### Introduction

Dewret allows certain workflows written in a dynamic
style to be rendered to a static representation.

Advantages of doing so include:

* **git-versionable workflows**: while code can be versioned, the changes of a dynamic workflow
  do not necessarily clearly correspond to changes in the executed workflow. This maintains
  a precise trackable history.
* **plan and play**: the workflow can be rapidly iterated, analysed and optimized before it
  is sent for real execution on expensive or restricted HPC hardware.
* **optimization**: creating the workflow explicitly opens up possibilities for static analysis
  and refactoring before real execution.
* **debugging**: a number of classes of workflow planning bugs will not appear until late
  in a simulation run that might take days or weeks. This catches them before startup.
* **continuous integration and testing**: complex dynamic workflows can be rapidly sense-checked
  in CI without needing all the hardware and internal algorithms present to run them.

### Documentation

For further information, see the [documentation](https://flaxandteal.github.io/dewret).

## Developer Set up

For development with conda, run the relevant script in `.set_up`