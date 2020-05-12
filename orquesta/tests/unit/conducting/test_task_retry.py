# Copyright 2019 Extreme Networks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from orquesta import conducting
from orquesta import events
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorTaskRetryTest(test_base.WorkflowConductorTest):
    def test_basic_retry(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}
        expected_tk2_action_spec = {"action": "core.noop", "input": None}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 1)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Assert task1 succeeded and the workflow execution progresses to task2.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.SUCCEEDED)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk2_action_spec)

        # Successful execution for task2.
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.SUCCEEDED)

        # Assert workflow completed successfully.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ["task1", "task2"]
        actual_task_sequence = [item["id"] for item in conductor.workflow_state.sequence]
        self.assertListEqual(expected_task_sequence, actual_task_sequence)

    def test_multiple_tasks_with_retry(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task3

          task2:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task4

          task3:
            action: core.noop

          task4:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}
        expected_tk2_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}
        expected_tk3_action_spec = {"action": "core.noop", "input": None}
        expected_tk4_action_spec = {"action": "core.noop", "input": None}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1 and task2.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 2)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[1]["id"], "task2")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])
        self.assertDictEqual(next_tasks[1]["actions"][0], expected_tk2_action_spec)
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.FAILED])

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 2)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 1)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Successful retry for task2.
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RETRYING)
        self.assertEqual(tk2_state["retry"]["count"], 3)
        self.assertEqual(tk2_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 2)
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.assertEqual(next_tasks[0]["delay"], 1)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk2_action_spec)
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])

        # Assert task1 succeeded and the workflow execution progresses to task3.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.SUCCEEDED)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 2)
        self.assertEqual(next_tasks[0]["id"], "task3")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk3_action_spec)

        # Assert task2 succeeded and the workflow execution progresses to task4.
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.SUCCEEDED)
        self.assertEqual(tk2_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 2)
        self.assertEqual(next_tasks[1]["id"], "task4")
        self.assertDictEqual(next_tasks[1]["actions"][0], expected_tk4_action_spec)

        # Successful execution for task3.
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        tk3_state = conductor.get_task_state_entry("task3", 0)
        self.assertEqual(tk3_state["status"], statuses.SUCCEEDED)

        # Successful execution for task4.
        self.forward_task_statuses(conductor, "task4", [statuses.RUNNING, statuses.SUCCEEDED])
        tk4_state = conductor.get_task_state_entry("task4", 0)
        self.assertEqual(tk4_state["status"], statuses.SUCCEEDED)

        # Assert workflow completed successfully.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ["task1", "task2", "task3", "task4"]
        actual_task_sequence = [item["id"] for item in conductor.workflow_state.sequence]
        self.assertListEqual(expected_task_sequence, actual_task_sequence)

    def test_retries_exhausted(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Failed retry #1 for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Failed retry #2 for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["tally"], 2)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Failed retry #3 for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["tally"], 3)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Assert task1 succeeded and the workflow execution progresses to task2.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.FAILED)
        self.assertEqual(tk1_state["retry"]["tally"], 3)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 0)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ["task1"]
        actual_task_sequence = [item["id"] for item in conductor.workflow_state.sequence]
        self.assertListEqual(expected_task_sequence, actual_task_sequence)

    def test_retries_exhausted_and_task_remediated(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
              - when: <% failed() %>
                do: task3
          task2:
            action: core.noop
          task3:
            action: core.echo message="BOOM!"
            next:
              - do: fail
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}
        expected_tk3_action_spec = {"action": "core.echo", "input": {"message": "BOOM!"}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Failed retry #1 for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Failed retry #2 for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["tally"], 2)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Failed retry #3 for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["tally"], 3)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Assert task1 failed and the workflow execution progresses to task3.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.FAILED)
        self.assertEqual(tk1_state["retry"]["tally"], 3)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task3")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk3_action_spec)

        # Successful execution for task3.
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        tk3_state = conductor.get_task_state_entry("task3", 0)
        self.assertEqual(tk3_state["status"], statuses.SUCCEEDED)

        # Assert workflow failed (manual under task3).
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # Assert there is only a single task1 and a single task3 in the task sequences.
        expected_task_sequence = ["task1", "task3", "fail"]
        actual_task_sequence = [item["id"] for item in conductor.workflow_state.sequence]
        self.assertListEqual(expected_task_sequence, actual_task_sequence)

    def test_retry_delay_with_task_delay_defined(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            delay: 2
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 2)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 1)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

    def test_no_retry_delay_with_task_delay_defined(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            delay: 2
            action: core.echo message="$RANDOM"
            retry:
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 2)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 0)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

    def test_workflow_cancellation_before_retry_running(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)

        # Request workflow cancellation.
        conductor.request_workflow_status(statuses.CANCELING)

        # Assert task1 is still retrying but not returned in get_next_tasks.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 0)

        # Assert workflow is canceled.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_workflow_cancellation_while_retry_running(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Start retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 1)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RUNNING)

        # Request workflow cancellation.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

        # Fail task1 and assert the task is not returned in get_next_tasks.
        self.forward_task_statuses(conductor, "task1", [statuses.FAILED])
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 0)

        # Assert workflow is canceled.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_workflow_cancellation_with_multiple_retries_not_all_running(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task3

          task2:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task4

          task3:
            action: core.noop

          task4:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1 and task2.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.FAILED])

        # Start retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RUNNING)

        # Request workflow cancellation.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

        # Fail tasks and assert workflow transition from canceling to canceled.
        self.forward_task_statuses(conductor, "task1", [statuses.FAILED])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RETRYING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_workflow_cancellation_with_multiple_retries_running(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task3

          task2:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task4

          task3:
            action: core.noop

          task4:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1 and task2.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.FAILED])

        # Start retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RUNNING)

        # Start retry for task2.
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RETRYING)
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RUNNING)

        # Request workflow cancellation.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

        # Fail tasks and assert workflow transition from canceling to canceled.
        self.forward_task_statuses(conductor, "task1", [statuses.FAILED])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)
        self.forward_task_statuses(conductor, "task2", [statuses.FAILED])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RETRYING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_workflow_pause_before_retry_running(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}
        expected_tk2_action_spec = {"action": "core.noop", "input": None}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)

        # Assert task1 is still retrying but not returned in get_next_tasks.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 0)

        # Assert workflow is canceled.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_status(statuses.RESUMING)

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 1)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Assert task1 succeeded and the workflow execution progresses to task2.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.SUCCEEDED)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk2_action_spec)

        # Successful execution for task2.
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.SUCCEEDED)

        # Assert workflow completed successfully.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ["task1", "task2"]
        actual_task_sequence = [item["id"] for item in conductor.workflow_state.sequence]
        self.assertListEqual(expected_task_sequence, actual_task_sequence)

    def test_workflow_pause_while_retry_running(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}
        expected_tk2_action_spec = {"action": "core.noop", "input": None}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Start retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 1)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

        # Fail task1 and assert the task is not returned in get_next_tasks.
        self.forward_task_statuses(conductor, "task1", [statuses.FAILED])
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 0)

        # Assert the workflow is paused.
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_status(statuses.RESUMING)

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 2)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 1)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Assert task1 succeeded and the workflow execution progresses to task2.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.SUCCEEDED)
        self.assertEqual(tk1_state["retry"]["tally"], 2)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk2_action_spec)

        # Successful execution for task2.
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.SUCCEEDED)

        # Assert workflow completed successfully.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ["task1", "task2"]
        actual_task_sequence = [item["id"] for item in conductor.workflow_state.sequence]
        self.assertListEqual(expected_task_sequence, actual_task_sequence)

    def test_workflow_pause_with_multiple_retries_not_all_running(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task3

          task2:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task4

          task3:
            action: core.noop

          task4:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1 and task2.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.FAILED])

        # Start retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

        # Fail tasks and assert workflow transition from pausing to paused.
        self.forward_task_statuses(conductor, "task1", [statuses.FAILED])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RETRYING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

    def test_workflow_pause_with_multiple_retries_running(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task3

          task2:
            action: core.echo message="$RANDOM"
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task4

          task3:
            action: core.noop

          task4:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1 and task2.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.FAILED])

        # Start retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RUNNING)

        # Start retry for task2.
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RETRYING)
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

        # Fail tasks and assert workflow transition from pausing to paused.
        self.forward_task_statuses(conductor, "task1", [statuses.FAILED])
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)
        self.forward_task_statuses(conductor, "task2", [statuses.FAILED])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.RETRYING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

    def test_retry_with_items(self):
        wf_def = """
        version: 1.0

        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum

        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2

          task2:
            action: core.noop
        """

        result = [{"stdout": "fee"}, {"stdout": "fi"}, {"stdout": "fo"}, {"stdout": "fum"}]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Assert task1 is returned in get_next_tasks.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Fail task1 and assert task1 is setup for retry.
        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.RUNNING)
            conductor.update_task_state("task1", 0, ac_ex_event)

        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.FAILED, None, result)
            conductor.update_task_state("task1", 0, ac_ex_event)

        # Assert task1 is setup for retry.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Successful retry for task1.
        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.RUNNING)
            conductor.update_task_state("task1", 0, ac_ex_event)

        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.SUCCEEDED, None, result)
            conductor.update_task_state("task1", 0, ac_ex_event)

        # Assert task1 succeeded and the workflow execution progresses to task2.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.SUCCEEDED)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task2")

        # Successful execution for task2.
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.SUCCEEDED)

        # Assert workflow completed successfully.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ["task1", "task2"]
        actual_task_sequence = [item["id"] for item in conductor.workflow_state.sequence]
        self.assertListEqual(expected_task_sequence, actual_task_sequence)

    def test_workflow_cancellation_with_retry_running_with_items(self):
        wf_def = """
        version: 1.0

        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum

        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2

          task2:
            action: core.noop
        """

        result = [{"stdout": "fee"}, {"stdout": "fi"}, {"stdout": "fo"}, {"stdout": "fum"}]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Assert task1 is returned in get_next_tasks.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Fail task1 and assert task1 is setup for retry.
        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.RUNNING)
            conductor.update_task_state("task1", 0, ac_ex_event)

        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.FAILED, None, result)
            conductor.update_task_state("task1", 0, ac_ex_event)

        # Assert task1 is setup for retry.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Start retry for task1.
        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.RUNNING)
            conductor.update_task_state("task1", 0, ac_ex_event)

        # Request workflow cancellation.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

        # Fail the retry for task1.
        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.FAILED)
            conductor.update_task_state("task1", 0, ac_ex_event)

        # Assert task1 is setup for retry.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.CANCELED)

        # Assert workflow is canceled.
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_workflow_pause_with_retry_running_with_items(self):
        wf_def = """
        version: 1.0

        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum

        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            retry:
              delay: 1
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2

          task2:
            action: core.noop
        """

        result = [{"stdout": "fee"}, {"stdout": "fi"}, {"stdout": "fo"}, {"stdout": "fum"}]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Assert task1 is returned in get_next_tasks.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Fail task1 and assert task1 is setup for retry.
        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.RUNNING)
            conductor.update_task_state("task1", 0, ac_ex_event)

        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.FAILED, None, result)
            conductor.update_task_state("task1", 0, ac_ex_event)

        # Assert task1 is setup for retry.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Start retry for task1.
        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.RUNNING)
            conductor.update_task_state("task1", 0, ac_ex_event)

        # Pause workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

        # Succeed the retry for task1.
        for i in range(0, 4):
            ac_ex_event = events.TaskItemActionExecutionEvent(i, statuses.FAILED)
            conductor.update_task_state("task1", 0, ac_ex_event)

        # Assert task1 is setup for retry.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)

        # Assert workflow is canceled.
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

    def test_bad_task_name(self):
        wf_def = """
        version: 1.0

        tasks:
          retry:
            action: core.echo message="$RANDOM"
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_inspection_errors = {
            "semantics": [
                {
                    "message": 'The task name "retry" is reserved with special function.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$",
                    "spec_path": "tasks.retry",
                }
            ]
        }

        spec = native_specs.WorkflowSpec(wf_def)

        self.assertDictEqual(spec.inspect(), expected_inspection_errors)

    def test_retry_command(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% failed() %>
                do: retry
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_tk1_action_spec = {"action": "core.echo", "input": {"message": "$RANDOM"}}
        expected_tk2_action_spec = {"action": "core.noop", "input": None}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.FAILED])

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.RETRYING)
        self.assertEqual(tk1_state["retry"]["count"], 3)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(next_tasks[0]["delay"], 0)
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Assert task1 succeeded and the workflow execution progresses to task2.
        tk1_state = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(tk1_state["status"], statuses.SUCCEEDED)
        self.assertEqual(tk1_state["retry"]["tally"], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.assertDictEqual(next_tasks[0]["actions"][0], expected_tk2_action_spec)

        # Successful execution for task2.
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])
        tk2_state = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(tk2_state["status"], statuses.SUCCEEDED)

        # Assert workflow completed successfully.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ["task1", "task2"]
        actual_task_sequence = [item["id"] for item in conductor.workflow_state.sequence]
        self.assertListEqual(expected_task_sequence, actual_task_sequence)
