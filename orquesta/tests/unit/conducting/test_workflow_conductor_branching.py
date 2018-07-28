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

    def test_join(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        tasks:
          # branch 1
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var1: 'xyz'
                do: task3

          # branch 2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var2: 123
                do: task3

          # adjoining branch
          task3:
            join: all
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var3: True
                do: task4
          task4:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Conduct task1 and check context and that there is no next tasks yet.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        expected_txsn_ctx = {'task3__0': {'srcs': [0], 'value': {'var1': 'xyz'}}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)

        # Conduct task2 and check next tasks and context.
        task_name = 'task2'
        next_task_name = 'task3'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_txsn_ctx = {'task3__0': {'srcs': [0, 1], 'value': {'var1': 'xyz', 'var2': 123}}}
        self.assertDictEqual(conductor.get_task_transition_contexts('task2'), expected_txsn_ctx)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = {'var1': 'xyz', 'var2': 123}
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        # Conduct task3 and check merged context.
        task_name = 'task3'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_init_ctx = {'srcs': [0, 1], 'value': expected_ctx_value}
        self.assertDictEqual(conductor.get_task_initial_context(task_name), expected_init_ctx)
        expected_ctx_val = {'var1': 'xyz', 'var2': 123, 'var3': True}
        expected_txsn_ctx = {'task4__0': {'srcs': [2], 'value': expected_ctx_val}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)

        # Conduct task4 and check final workflow state.
        task_name = 'task4'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        # Check workflow state and context.
        expected_ctx_value = {'var1': 'xyz', 'var2': 123, 'var3': True}
        expected_ctx_entry = {'src': [3], 'term': True, 'value': expected_ctx_value}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_ctx_entry)

    def test_join_with_no_input_and_no_context_changes(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        tasks:
          # branch 1
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3

          # branch 2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3

          # adjoining branch
          task3:
            join: all
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task4
          task4:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Conduct task2 and check context and that there is no next tasks yet.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        expected_txsn_ctx = {'task3__0': {'srcs': [], 'value': {}}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)

        # Conduct task2 and check next tasks and context.
        task_name = 'task2'
        next_task_name = 'task3'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_txsn_ctx = {'task3__0': {'srcs': [], 'value': {}}}
        self.assertDictEqual(conductor.get_task_transition_contexts('task2'), expected_txsn_ctx)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = {}
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        # Conduct task3 and check merged context.
        task_name = 'task3'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_init_ctx = {'srcs': [], 'value': {}}
        self.assertDictEqual(conductor.get_task_initial_context(task_name), expected_init_ctx)
        expected_txsn_ctx = {'task4__0': {'srcs': [], 'value': {}}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)

        # Conduct task4 and check final workflow state.
        task_name = 'task4'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        # Check workflow state and context.
        expected_ctx_entry = {'src': [3], 'term': True, 'value': {}}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_ctx_entry)

    def test_join_with_input_and_no_context_changes(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        input:
          - var1

        tasks:
          # branch 1
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3

          # branch 2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3

          # adjoining branch
          task3:
            join: all
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task4
          task4:
            action: core.noop
        """

        inputs = {'var1': 'xyz'}
        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec, inputs=inputs)
        conductor.request_workflow_state(states.RUNNING)

        # Conduct task2 and check context and that there is no next tasks yet.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        expected_txsn_ctx = {'task3__0': {'srcs': [], 'value': inputs}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)

        # Conduct task2 and check next tasks and context.
        task_name = 'task2'
        next_task_name = 'task3'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_txsn_ctx = {'task3__0': {'srcs': [], 'value': inputs}}
        self.assertDictEqual(conductor.get_task_transition_contexts('task2'), expected_txsn_ctx)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = inputs
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        # Conduct task3 and check merged context.
        task_name = 'task3'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_init_ctx = {'srcs': [], 'value': inputs}
        self.assertDictEqual(conductor.get_task_initial_context(task_name), expected_init_ctx)
        expected_txsn_ctx = {'task4__0': {'srcs': [], 'value': inputs}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)

        # Conduct task4 and check final workflow state.
        task_name = 'task4'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        # Check workflow state and context.
        expected_ctx_entry = {'src': [3], 'term': True, 'value': inputs}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_ctx_entry)

    def test_parallel(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        tasks:
          # branch 1
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var1: 'xyz'
                do: task2
          task2:
            action: core.noop

          # branch 2
          task3:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var2: 123
                do: task4
          task4:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Conduct task1 and check context.
        task_name = 'task1'
        next_task_name = 'task2'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_ctx_value = {'var1': 'xyz'}
        expected_txsn_ctx = {'task2__0': {'srcs': [0], 'value': expected_ctx_value}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        # Conduct task3 and check context.
        task_name = 'task3'
        next_task_name = 'task4'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        expected_ctx_value = {'var2': 123}
        expected_txsn_ctx = {'task4__0': {'srcs': [1], 'value': expected_ctx_value}}
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), expected_txsn_ctx)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        # Conduct task2 and check context.
        task_name = 'task2'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), {})
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        # Conduct task4 and check context.
        task_name = 'task4'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name), {})
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        # Check workflow state and context.
        expected_ctx_entry = {'src': [2, 3], 'term': True, 'value': {'var1': 'xyz', 'var2': 123}}
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_ctx_entry)
