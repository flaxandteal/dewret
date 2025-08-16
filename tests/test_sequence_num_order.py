"""Check sequential numbers are added in task order."""

import yaml
from dewret.tasks import construct, task, workflow
from dewret.renderers.cwl import render, TransparentOrderedDict
from dewret.workflow import NestedStep, BaseStep, _SEQUENCE_NUM
from concurrent.futures import ThreadPoolExecutor, as_completed
from dewret.core import SequenceManager, set_configuration
import time

class OrderedDictLoader(yaml.SafeLoader):
    def construct_mapping(self, node):
        self.flatten_mapping(node)
        pairs = self.construct_pairs(node)

        return TransparentOrderedDict(pairs)

OrderedDictLoader.add_constructor('tag:yaml.org,2002:map', OrderedDictLoader.construct_mapping)
yaml_ordered_load = lambda yml: yaml.load(yml, Loader=OrderedDictLoader)

@task()
def increment(num: int) -> int:
    """Simple task to increment a number."""
    return num + 1


@task()
def sum(left: int, right: int) -> int:
    """A task to add two numbers."""
    return left + right


@task()
def sum_all(a: int, b: int, c: int, d: int) -> int:
    """A task to add four numbers."""
    return a + b + c + d


@workflow()
def example_workflow() -> int:
    """Test workflow with multiple tasks."""
    step1 = increment(num=1)
    step2 = increment(num=5)
    step3 = sum(left=step1, right=step2)
    return step3


@workflow()
def linear_workflow() -> int:
    """A workflow to test ordered output for steps dependent upon the previous step."""
    step1 = increment(num=1)
    step2 = increment(num=step1)
    step3 = increment(num=step2)
    return step3


@workflow()
def long_workflow() -> int:
    """A longer workflow with several steps to test ordering."""
    step1 = increment(num=1)
    step2 = increment(num=step1)
    step3 = increment(num=step2)
    step4 = sum(left=step3, right=step2)
    step5 = sum(left=4, right=6)
    step6 = sum(left=step4, right=step5)
    step7 = increment(num=step6)
    step8 = increment(num=step7)
    return step8


def output() -> int:
    """A function to run several tasks without a workflow."""
    step1 = increment(num=1)
    step2 = increment(num=5)
    step3 = increment(num=7)
    step4 = sum(left=step1, right=step2)
    step5 = sum(left=step4, right=step3)
    return step5


@workflow()
def combined_workflow() -> int:
    """A workflow to test ordered output for nested structures."""
    step1 = linear_workflow()
    step2 = long_workflow()
    step3 = sum(left=step1, right=step2)
    return step3

@workflow()
def level_1_workflow() -> int:
    """Base level workflow - simplest tasks."""
    step1 = increment(num=1)
    step2 = increment(num=step1)
    return step2

@workflow()
def level_2_workflow() -> int:
    """Second level - contains level 1 workflows."""
    step1 = level_1_workflow()
    step2 = increment(num=step1)
    step3 = sum(left=step2, right=step1)
    return step3

@workflow()
def level_3_workflow() -> int:
    """Third level - contains level 2 workflows."""
    step1 = level_2_workflow()
    step2 = sum(left=step1, right=step1)
    step3 = increment(num=step2)
    return step3

@workflow()
def level_4_workflow() -> int:
    """Fourth level - contains level 3 workflows (4 workflows deep)."""
    step1 = level_3_workflow()
    step2 = increment(num=step1)
    step3 = sum(left=step1, right=step1)
    step4 = increment(num=step3)
    step5 = sum(left=step2, right=step4)
    return step5

@workflow()
def level_4_workflow_wide() -> int:
    """Fourth level - contains level 3 workflows (4 workflows deep)."""
    step1 = level_3_workflow()
    step2 = increment(num=7)
    step3 = sum(left=step1, right=step1)
    step4 = increment(num=2)
    step5 = sum_all(a=step1, b=step2, c=step3, d=step4)
    return step5

def check_sequence_numbers_in_sequence(
        sequenced_steps: dict[str, BaseStep],
    ) -> tuple[bool, list[BaseStep]]:
        """Check if sequence numbers are not None and are incrementing correctly."""
        steps = list(sequenced_steps.values())

        for i in range(len(steps) - 1):
            current_num = steps[i].__sequence_num__
            next_num = steps[i + 1].__sequence_num__

            # Ensure both are not None before comparison
            if current_num is None or next_num is None:
                return (False, steps)
            if current_num >= next_num:
                return (False, steps)

        return (True, steps)

def test_sequence_numbers_are_sequential_in_example_workflow() -> None:
    """Test to check if the sequence numbers are correctly ordered in the example workflow."""
    workflow = construct(example_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep)

    sequenced_steps = nested_step.subworkflow.sequenced_steps

    assert (
        len(sequenced_steps) == 3
    ), f"Expected 3 steps received {len(sequenced_steps)}"

    in_sequence = check_sequence_numbers_in_sequence(sequenced_steps)

    assert (
        in_sequence[0] == True
    ), f"The step sequence numbers did not iterate sequentially {in_sequence[1]}"


def test_sequence_number_in_linear_workflow() -> None:
    """Test to check if sequence numbers are correctly ordered in the linear workflow."""
    workflow = construct(linear_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep)

    sequenced_steps = nested_step.subworkflow.sequenced_steps

    assert (
        len(sequenced_steps) == 3
    ), f"Expected 3 steps received {len(sequenced_steps)}"

    in_sequence = check_sequence_numbers_in_sequence(sequenced_steps)

    assert (
        in_sequence[0] == True
    ), f"The step sequence numbers did not iterate sequentially {in_sequence[1]}"


def test_sequence_number_in_long_workflow() -> None:
    """Test to check if sequence numbers are correctly ordered in the long workflow."""
    workflow = construct(long_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep)

    sequenced_steps = nested_step.subworkflow.sequenced_steps

    assert (
        len(sequenced_steps) == 8
    ), f"Expected 8 steps received {len(sequenced_steps)}"

    in_sequence = check_sequence_numbers_in_sequence(sequenced_steps)

    assert (
        in_sequence[0] == True
    ), f"The step sequence numbers did not iterate sequentially {in_sequence[1]}"


def test_sequence_number_in_combined_workflow() -> None:
    """Test to check if sequence numbers are correctly ordered in the long workflow."""
    workflow = construct(combined_workflow(), simplify_ids=True)

    nested_step = list(workflow.steps)[0]

    assert isinstance(nested_step, NestedStep)

    sequenced_steps = nested_step.subworkflow.sequenced_steps

    assert (
        len(sequenced_steps) == 3
    ), f"Expected 3 steps received {len(sequenced_steps)}"

    in_sequence = check_sequence_numbers_in_sequence(sequenced_steps)

    assert (
        in_sequence[0] == True
    ), f"The step sequence numbers did not iterate sequentially {in_sequence[1]}"


def test_render_outputs_list_in_order_for_example_workflow() -> None:
    """Test to see if the yaml renders with the list in the correct order."""
    workflow = construct(example_workflow(), simplify_ids=True)

    rendered = render(workflow, sort_steps=True)["example_workflow-1"]

    assert rendered == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            { "out": { "label": "out", "type": "int", "outputSource": "sum-1-1/out" } },
        "steps":
            {
            "increment-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-1":
                {
                "run": "increment",
                "in": { "num": { "default": 5 } },
                "out": ["out"],
                },
            "sum-1-1":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "increment-1-2/out" },
                    "right": { "source": "increment-1-1/out" },
                    },
                "out": ["out"],
                },
            },
        }
    """)


def test_render_linear_outputs_list_in_order() -> None:
    """Test to see if the yaml renders the linear task list in the correct order."""
    workflow = construct(linear_workflow(), simplify_ids=True)

    rendered = render(workflow, sort_steps=True)["linear_workflow-1"]

    assert rendered == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                { "label": "out", "type": "int", "outputSource": "increment-1-1/out" },
            },
        "steps":
            {
            "increment-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-2/out" } },
                "out": ["out"],
                },
            "increment-1-1":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-3/out" } },
                "out": ["out"],
                },
            },
        }
    """)


def test_render_long_workflow_outputs_list_in_order() -> None:
    """Test to see if the yaml renders the linear task list in the correct order."""
    workflow = construct(long_workflow(), simplify_ids=True)

    rendered = render(workflow, sort_steps=True)["long_workflow-1"]

    assert rendered == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                { "label": "out", "type": "int", "outputSource": "increment-1-5/out" },
            },
        "steps":
            {
            "increment-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-2/out" } },
                "out": ["out"],
                },
            "increment-1-1":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-3/out" } },
                "out": ["out"],
                },
            "sum-1-3":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "increment-1-1/out" },
                    "right": { "source": "increment-1-3/out" },
                    },
                "out": ["out"],
                },
            "sum-1-2":
                {
                "run": "sum",
                "in": { "left": { "default": 4 }, "right": { "default": 6 } },
                "out": ["out"],
                },
            "sum-1-1":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "sum-1-3/out" },
                    "right": { "source": "sum-1-2/out" },
                    },
                "out": ["out"],
                },
            "increment-1-4":
                {
                "run": "increment",
                "in": { "num": { "source": "sum-1-1/out" } },
                "out": ["out"],
                },
            "increment-1-5":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-4/out" } },
                "out": ["out"],
                },
            },
        }
    """)

def test_render_nested_workflow_outputs_list_in_order() -> None:
    """Test to see if the yaml renders the linear task list in the correct order."""
    workflow = construct(combined_workflow(), simplify_ids=True)

    rendered_combined = render(workflow, sort_steps=True)["combined_workflow-1"]
    rendered_linear = render(workflow, sort_steps=True)["linear_workflow-1-1"]
    rendered_long = render(workflow, sort_steps=True)["long_workflow-1-1"]
    
    assert rendered_combined == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            { "out": { "label": "out", "type": "int", "outputSource": "sum-1-1/out" } },
        "steps":
            {
            "linear_workflow-1-1":
                { "run": "linear_workflow", "in": {}, "out": ["out"] },
            "long_workflow-1-1": { "run": "long_workflow", "in": {}, "out": ["out"] },
            "sum-1-1":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "linear_workflow-1-1/out" },
                    "right": { "source": "long_workflow-1-1/out" },
                    },
                "out": ["out"],
                },
            },
        }
    """)

    assert rendered_linear == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                {
                "label": "out",
                "type": "int",
                "outputSource": "increment-1-1-1/out",
                },
            },
        "steps":
            {
            "increment-1-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-2/out" } },
                "out": ["out"],
                },
            "increment-1-1-1":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-3/out" } },
                "out": ["out"],
                },
            },
        }
    """)

    assert rendered_long == yaml_ordered_load("""
                {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs":
            {
            "out":
                {
                "label": "out",
                "type": "int",
                "outputSource": "increment-1-1-5/out",
                },
            },
        "steps":
            {
            "increment-1-1-2":
                {
                "run": "increment",
                "in": { "num": { "default": 1 } },
                "out": ["out"],
                },
            "increment-1-1-3":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-2/out" } },
                "out": ["out"],
                },
            "increment-1-1-1":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-3/out" } },
                "out": ["out"],
                },
            "sum-1-1-3":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "increment-1-1-1/out" },
                    "right": { "source": "increment-1-1-3/out" },
                    },
                "out": ["out"],
                },
            "sum-1-1-2":
                {
                "run": "sum",
                "in": { "left": { "default": 4 }, "right": { "default": 6 } },
                "out": ["out"],
                },
            "sum-1-1-1":
                {
                "run": "sum",
                "in":
                    {
                    "left": { "source": "sum-1-1-3/out" },
                    "right": { "source": "sum-1-1-2/out" },
                    },
                "out": ["out"],
                },
            "increment-1-1-4":
                {
                "run": "increment",
                "in": { "num": { "source": "sum-1-1-1/out" } },
                "out": ["out"],
                },
            "increment-1-1-5":
                {
                "run": "increment",
                "in": { "num": { "source": "increment-1-1-4/out" } },
                "out": ["out"],
                },
            },
        }
    """)

def test_render_level_4_workflow_outputs_list_in_order() -> None:
    """Test to see if the yaml renders the 4-level deep workflow in the correct order."""
    workflow = construct(level_4_workflow(), simplify_ids=True)

    rendered = render(workflow, sort_steps=True)
    
    # Test level_1_workflow (deepest level)
    rendered_level_1 = rendered["level_1_workflow-1-1-1-1"]
    assert rendered_level_1 == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs": {
            "out": {
                "label": "out",
                "type": "int",
                "outputSource": "increment-1-1-1-1-2/out"
            }
        },
        "steps": {
            "increment-1-1-1-1-1": {
                "run": "increment",
                "in": {"num": {"default": 1}},
                "out": ["out"]
            },
            "increment-1-1-1-1-2": {
                "run": "increment",
                "in": {"num": {"source": "increment-1-1-1-1-1/out"}},
                "out": ["out"]
            }
        }
        }
    """)

    # Test level_2_workflow
    rendered_level_2 = rendered["level_2_workflow-1-1-1"]
    assert rendered_level_2 == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs": {
            "out": {
                "label": "out",
                "type": "int",
                "outputSource": "sum-1-1-1-1/out"
            }
        },
        "steps": {
            "level_1_workflow-1-1-1-1": {
                "run": "level_1_workflow",
                "in": {},
                "out": ["out"]
            },
            "increment-1-1-1-1": {
                "run": "increment",
                "in": {"num": {"source": "level_1_workflow-1-1-1-1/out"}},
                "out": ["out"]
            },
            "sum-1-1-1-1": {
                "run": "sum",
                "in": {
                    "left": {"source": "increment-1-1-1-1/out"},
                    "right": {"source": "level_1_workflow-1-1-1-1/out"}
                },
                "out": ["out"]
            }
        }
        }
    """)

    # Test level_3_workflow
    rendered_level_3 = rendered["level_3_workflow-1-1"]
    assert rendered_level_3 == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs": {
            "out": {
                "label": "out",
                "type": "int",
                "outputSource": "increment-1-1-1/out"
            }
        },
        "steps": {
            "level_2_workflow-1-1-1": {
                "run": "level_2_workflow",
                "in": {},
                "out": ["out"]
            },
            "sum-1-1-1": {
                "run": "sum",
                "in": {
                    "left": {"source": "level_2_workflow-1-1-1/out"},
                    "right": {"source": "level_2_workflow-1-1-1/out"}
                },
                "out": ["out"]
            },
            "increment-1-1-1": {
                "run": "increment",
                "in": {"num": {"source": "sum-1-1-1/out"}},
                "out": ["out"]
            }
        }
        }
    """)

    # Test level_4_workflow (top level)
    rendered_level_4 = rendered["level_4_workflow-1"]
    assert rendered_level_4 == yaml_ordered_load("""
        {
        "cwlVersion": 1.2,
        "class": "Workflow",
        "inputs": {},
        "outputs": {
            "out": {
                "label": "out",
                "type": "int",
                "outputSource": "sum-1-2/out"
            }
        },
        "steps": {
            "level_3_workflow-1-1": {
                "run": "level_3_workflow",
                "in": {},
                "out": ["out"]
            },
            "increment-1-1": {
                "run": "increment",
                "in": {"num": {"source": "level_3_workflow-1-1/out"}},
                "out": ["out"]
            },
            "sum-1-1": {
                "run": "sum",
                "in": {
                    "left": {"source": "level_3_workflow-1-1/out"},
                    "right": {"source": "level_3_workflow-1-1/out"}
                },
                "out": ["out"]
            },
            "increment-1-2": {
                "run": "increment",
                "in": {"num": {"source": "sum-1-1/out"}},
                "out": ["out"]
            },
            "sum-1-2": {
                "run": "sum",
                "in": {
                    "left": {"source": "increment-1-1/out"},
                    "right": {"source": "increment-1-2/out"}
                },
                "out": ["out"]
            }
        }
        }
    """)

    # Verify that we have all 4 levels of workflows
    expected_workflows = {
        "level_1_workflow-1-1-1-1",
        "level_2_workflow-1-1-1", 
        "level_3_workflow-1-1",
        "level_4_workflow-1"
    }
    
    actual_workflows = set(rendered.keys())
    assert expected_workflows.issubset(actual_workflows), (
        f"Expected workflows {expected_workflows} to be present in rendered output. "
        f"Got: {actual_workflows}"
    )
    
def test_sequence_num_increments_independently_per_thread() -> None:
    """Test that _SEQUENCE_NUM increments independently on each thread."""
    # Results storage with thread identification
    results = {}
    
    def thread_worker(thread_id: int, iterations: int = 5) -> list[int]:
        """Worker function that increments sequence number multiple times."""
        thread_results = []
        
        # Use the sequence context to reset for this thread
        with SequenceManager.sequence_context(_SEQUENCE_NUM):
            for i in range(iterations):
                # Get and increment the sequence number
                seq_num = SequenceManager.get_sequence_num(_SEQUENCE_NUM)
                thread_results.append(seq_num)
                
                # Add a small delay to increase chance of thread interleaving
                time.sleep(0.001)
        
        return thread_results
    
    # Run multiple threads concurrently
    num_threads = 5
    iterations_per_thread = 5
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit all thread workers
        future_to_thread = {
            executor.submit(thread_worker, thread_id, iterations_per_thread): thread_id
            for thread_id in range(num_threads)
        }
        
        # Collect results
        for future in as_completed(future_to_thread):
            thread_id = future_to_thread[future]
            results[thread_id] = future.result()
    
    # Verify that each thread started from 0 and incremented independently
    for thread_id, thread_results in results.items():
        # Each thread should start from 0 and increment sequentially
        expected = list(range(iterations_per_thread))
        assert thread_results == expected, (
            f"Thread {thread_id} should have sequence numbers {expected}, "
            f"but got {thread_results}"
        )
    
    # Verify all threads produced the same pattern independently
    all_results = list(results.values())
    for i, thread_results in enumerate(all_results):
        assert thread_results == all_results[0], (
            f"All threads should produce the same sequence pattern, "
            f"but thread {i} differs: {thread_results} vs {all_results[0]}"
        )

def test_sequence_context_isolation() -> None:
    """Test that sequence_context properly isolates thread contexts."""
    def get_current_sequence() -> int:
        """Helper to get current sequence number without incrementing."""
        return _SEQUENCE_NUM.get()
    
    def increment_sequence() -> int:
        """Helper to increment and return sequence number."""
        return SequenceManager.get_sequence_num(_SEQUENCE_NUM)
    
    # Test that different contexts are isolated
    results = []
    
    def context_worker(worker_id: int) -> None:
        """Create a dict for each worker displaying the incremented values."""
        with SequenceManager.sequence_context(_SEQUENCE_NUM):
            # Should start at 0 for each context
            initial = get_current_sequence()
            first_increment = increment_sequence()
            second_increment = increment_sequence()
            
            results.append({
                'worker_id': worker_id,
                'initial': initial,
                'first': first_increment,
                'second': second_increment
            })
    
    # Run multiple workers in sequence (not parallel) to verify isolation
    for i in range(3):
        context_worker(i)
    
    # Each worker should have the same sequence: 0, 0, 1
    for result in results:
        assert result['initial'] == 0, f"Worker {result['worker_id']} should start at 0"
        assert result['first'] == 0, f"Worker {result['worker_id']} first increment should be 0"
        assert result['second'] == 1, f"Worker {result['worker_id']} second increment should be 1"

def test_render_level_4_workflow_wide_outputs_list_in_order() -> None:
    """Test to see if the yaml renders the 4-level deep workflow in the correct order."""
    with set_configuration(flatten_all_nested=True):
        workflow = construct(level_4_workflow_wide(), simplify_ids=True)
        rendered = render(workflow, sort_steps=True)
    
    # Test level_1_workflow (deepest level)
    import json
    with open('/tmp/1.yaml', 'w') as f:
        json.dump(rendered, f, indent=2)

    rendered_level_1 = rendered["__root__"]
    assert rendered_level_1 == yaml_ordered_load("""
    {
    "cwlVersion": 1.2,
    "class": "Workflow",
    "inputs": {},
    "outputs": {
      "out": {
        "label": "out",
        "type": "int",
        "outputSource": "sum_all-1/out"
      }
    },
    "steps": {
      "increment-6": {
        "run": "increment",
        "in": {
          "num": {
            "default": 7
          }
        },
        "out": [
          "out"
        ]
      },
      "increment-2": {
        "run": "increment",
        "in": {
          "num": {
            "default": 1
          }
        },
        "out": [
          "out"
        ]
      },
      "increment-1": {
        "run": "increment",
        "in": {
          "num": {
            "source": "increment-4/out"
          }
        },
        "out": [
          "out"
        ]
      },
      "sum-1": {
        "run": "sum",
        "in": {
          "left": {
            "source": "sum-2/out"
          },
          "right": {
            "source": "sum-2/out"
          }
        },
        "out": [
          "out"
        ]
      },
      "sum-3": {
        "run": "sum",
        "in": {
          "left": {
            "source": "increment-5/out"
          },
          "right": {
            "source": "increment-5/out"
          }
        },
        "out": [
          "out"
        ]
      },
      "sum_all-1": {
        "run": "sum_all",
        "in": {
          "a": {
            "source": "increment-5/out"
          },
          "b": {
            "source": "increment-6/out"
          },
          "c": {
            "source": "sum-3/out"
          },
          "d": {
            "source": "increment-3/out"
          }
        },
        "out": [
          "out"
        ]
      },
      "increment-4": {
        "run": "increment",
        "in": {
          "num": {
            "source": "increment-2/out"
          }
        },
        "out": [
          "out"
        ]
      },
      "increment-5": {
        "run": "increment",
        "in": {
          "num": {
            "source": "sum-1/out"
          }
        },
        "out": [
          "out"
        ]
      },
      "sum-2": {
        "run": "sum",
        "in": {
          "left": {
            "source": "increment-1/out"
          },
          "right": {
            "source": "increment-4/out"
          }
        },
        "out": [
          "out"
        ]
      },
      "increment-3": {
        "run": "increment",
        "in": {
          "num": {
            "default": 2
          }
        },
        "out": [
          "out"
        ]
      }
    }
  }
    """)
