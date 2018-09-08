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
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base


class WorkflowConductorExtendedTest(base.WorkflowConductorTest):

    def test_self_looping(self):
        wf_def = """
        version: 1.0

        description: A basic workflow with cycle.

        vars:
          - loop: True

        tasks:
          task1:
            action: core.noop
            next:
              - do: task2
          task2:
            action: core.noop
            next:
              - when: <% ctx(loop) = true %>
                publish:
                  - loop: False
                do: task2
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Conduct task1 and check context and that there is no next tasks yet.
        task_name = 'task1'
        next_task_name = 'task2'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_ctx_value = {'loop': True}
        expected_txsn_ctx = {'task2__0': {'srcs': [], 'value': expected_ctx_value}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        # Conduct task2 and check next tasks and context.
        task_name = 'task2'
        next_task_name = 'task2'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_ctx_value = {'loop': False}
        expected_txsn_ctx = {'task2__0': {'srcs': [1], 'value': expected_ctx_value}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        # Conduct task2 and check next tasks and context.
        task_name = 'task2'
        next_task_name = 'task2'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assert_task_list(conductor.get_next_tasks(task_name), [])

        # Check workflow state and context.
        expected_ctx_value = {'loop': False}
        expected_ctx_entry = {'src': [2], 'term': True, 'value': expected_ctx_value}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_ctx_entry)
