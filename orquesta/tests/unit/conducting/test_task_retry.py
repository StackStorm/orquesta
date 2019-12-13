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

        expected_tk1_action_spec = {'action': 'core.echo', 'input': {'message': '$RANDOM'}}
        expected_tk2_action_spec = {'action': 'core.noop', 'input': None}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RETRYING)
        self.assertEqual(tk1_state['retry']['count'], 3)
        self.assertEqual(tk1_state['retry']['tally'], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertEqual(next_tasks[0]['delay'], 1)
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])

        # Assert task1 succeeded and the workflow execution progresses to task2.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.SUCCEEDED)
        self.assertEqual(tk1_state['retry']['tally'], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task2')
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk2_action_spec)

        # Successful execution for task2.
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING, statuses.SUCCEEDED])
        tk2_state = conductor.get_task_state_entry('task2', 0)
        self.assertEqual(tk2_state['status'], statuses.SUCCEEDED)

        # Assert workflow completed successfully.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ['task1', 'task2']
        actual_task_sequence = [item['id'] for item in conductor.workflow_state.sequence]
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

        expected_tk1_action_spec = {'action': 'core.echo', 'input': {'message': '$RANDOM'}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])

        # Failed retry #1 for task1.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RETRYING)
        self.assertEqual(tk1_state['retry']['count'], 3)
        self.assertEqual(tk1_state['retry']['tally'], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])

        # Failed retry #2 for task1.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RETRYING)
        self.assertEqual(tk1_state['retry']['tally'], 2)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])

        # Failed retry #3 for task1.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RETRYING)
        self.assertEqual(tk1_state['retry']['tally'], 3)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])

        # Assert task1 succeeded and the workflow execution progresses to task2.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.FAILED)
        self.assertEqual(tk1_state['retry']['tally'], 3)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 0)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # Assert there is only a single task1 in the task sequences.
        expected_task_sequence = ['task1']
        actual_task_sequence = [item['id'] for item in conductor.workflow_state.sequence]
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

        expected_tk1_action_spec = {'action': 'core.echo', 'input': {'message': '$RANDOM'}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertEqual(next_tasks[0]['delay'], 2)
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RETRYING)
        self.assertEqual(tk1_state['retry']['count'], 3)
        self.assertEqual(tk1_state['retry']['tally'], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertEqual(next_tasks[0]['delay'], 1)
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])

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

        expected_tk1_action_spec = {'action': 'core.echo', 'input': {'message': '$RANDOM'}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertEqual(next_tasks[0]['delay'], 2)
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])

        # Successful retry for task1.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RETRYING)
        self.assertEqual(tk1_state['retry']['count'], 3)
        self.assertEqual(tk1_state['retry']['tally'], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertEqual(next_tasks[0]['delay'], 0)
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])

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

        expected_tk1_action_spec = {'action': 'core.echo', 'input': {'message': '$RANDOM'}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RETRYING)

        # Request workflow cancellation.
        conductor.request_workflow_status(statuses.CANCELING)

        # Assert task1 is canceled and not returned in get_next_tasks.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.CANCELED)
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

        expected_tk1_action_spec = {'action': 'core.echo', 'input': {'message': '$RANDOM'}}

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Failed execution for task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])

        # Start retry for task1.
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RETRYING)
        self.assertEqual(tk1_state['retry']['count'], 3)
        self.assertEqual(tk1_state['retry']['tally'], 1)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task1')
        self.assertEqual(next_tasks[0]['delay'], 1)
        self.assertDictEqual(next_tasks[0]['actions'][0], expected_tk1_action_spec)
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        tk1_state = conductor.get_task_state_entry('task1', 0)
        self.assertEqual(tk1_state['status'], statuses.RUNNING)

        # Request workflow cancellation.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

        # Fail task1 and assert the task is not returned in get_next_tasks.
        self.forward_task_statuses(conductor, 'task1', [statuses.FAILED])
        self.assertEqual(tk1_state['status'], statuses.FAILED)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 0)

        # Assert workflow is canceled.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)
