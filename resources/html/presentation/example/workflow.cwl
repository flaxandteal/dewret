class: Workflow
cwlVersion: v1.2
inputs:
  my_task-1-input:
    default: value
    label: input
    type: string
outputs:
  out:
    label: out
    outputSource: my_task-1/out
    type: string
steps:
  my_task-1:
    in:
      input:
        source: my_task-1-input
    out:
    - out
    run: my_task

