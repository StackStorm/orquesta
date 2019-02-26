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
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base


class WorkflowConductorContextTest(base.WorkflowConductorTest):

    def test_get_task_state_at_various_locations(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: task_state_at_publish=<% task_state(task1) %>
                do: task2
          task2:
            action: core.echo
            input:
              message: <% task_state(task1) %>

        output:
          - task_state_at_publish: <% ctx().task_state_at_publish %>
          - task_state_at_output: <% task_state(task1) %>
        """

        expected_errors = []
        expected_output = {
            'task_state_at_publish': 'succeeded',
            'task_state_at_output': 'succeeded'
        }

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.assertListEqual(conductor.errors, expected_errors)

        # Complete task1.
        self.forward_task_states(conductor, 'task1', [states.RUNNING, states.SUCCEEDED])

        # Get next tasks and ensure task_state return expected result in task rendering.
        task2_ex_req = conductor.get_next_tasks()[0]
        self.assertEqual(task2_ex_req['id'], 'task2')
        self.assertEqual(task2_ex_req['actions'][0]['action'], 'core.echo')
        self.assertEqual(task2_ex_req['actions'][0]['input']['message'], states.SUCCEEDED)

        # Complete task2.
        self.forward_task_states(conductor, 'task2', [states.RUNNING, states.SUCCEEDED])

        # Check workflow status and output.
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_get_task_state_of_tasks_along_split(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            next:
              - do: task3
          task2:
            action: core.noop
            next:
              - do: task3
          task3:
            action: core.noop
            next:
              - publish:
                  - task1_state: <% task_state(task1) %>
                  - task2_state: <% task_state(task2) %>
                  - task3_state: <% task_state(task3) %>

        output:
          - task1_state: <% ctx(task1_state) %>
          - task2_state: <% ctx(task2_state) %>
          - task3_state: <% ctx(task3_state) %>
        """

        expected_errors = []
        expected_output = {
            'task1_state': 'succeeded',
            'task2_state': 'succeeded',
            'task3_state': 'succeeded'
        }

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.assertListEqual(conductor.errors, expected_errors)

        # Complete tasks
        self.forward_task_states(conductor, 'task1', [states.RUNNING, states.SUCCEEDED])
        self.forward_task_states(conductor, 'task2', [states.RUNNING, states.SUCCEEDED])
        self.forward_task_states(conductor, 'task3', [states.RUNNING, states.SUCCEEDED], route=1)
        self.forward_task_states(conductor, 'task3', [states.RUNNING, states.SUCCEEDED], route=2)

        # Check workflow status and output.
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_get_task_state_of_tasks_along_splits(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2
              - when: <% failed() %>
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
              - when: <% failed() %>
                do: task3
          task3:
            action: core.noop
            next:
              - publish:
                  - task1_state: <% task_state(task1) %>
                  - task2_state: <% task_state(task2) %>
                  - task3_state: <% task_state(task3) %>

        output:
          - task1_state: <% ctx(task1_state) %>
          - task2_state: <% ctx(task2_state) %>
          - task3_state: <% ctx(task3_state) %>
        """

        expected_errors = []
        expected_output = {
            'task1_state': 'succeeded',
            'task2_state': 'succeeded',
            'task3_state': 'succeeded'
        }

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.assertListEqual(conductor.errors, expected_errors)

        # Complete tasks
        self.forward_task_states(conductor, 'task1', [states.RUNNING, states.SUCCEEDED])
        self.forward_task_states(conductor, 'task2', [states.RUNNING, states.SUCCEEDED], route=1)
        self.forward_task_states(conductor, 'task3', [states.RUNNING, states.SUCCEEDED], route=2)

        # Check workflow status and output.
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
