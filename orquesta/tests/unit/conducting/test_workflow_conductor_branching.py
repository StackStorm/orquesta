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

import copy

from orquesta import conducting
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base
from orquesta.utils import dictionary as dx


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

        # Conduct task1 and task2 then check context and next tasks.
        self.forward_task_states(conductor, 'task1', [states.RUNNING])
        self.forward_task_states(conductor, 'task2', [states.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), {})

        # Succeed task1 and check context.
        self.forward_task_states(conductor, 'task1', [states.SUCCEEDED])
        expected_task_ctx = {'var1': 'xyz'}
        expected_txsn_ctx = {'task3__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, has_next_task=False)

        # Conduct task2 and check next tasks and context.
        self.forward_task_states(conductor, 'task2', [states.SUCCEEDED])
        expected_task_ctx = {'var1': 'xyz', 'var2': 123}
        expected_txsn_ctx = {'task3__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task2', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task3', expected_task_ctx)

        # Conduct task3 and check merged context.
        self.forward_task_states(conductor, 'task3', [states.RUNNING, states.SUCCEEDED])
        expected_task_init_ctx = expected_task_ctx
        self.assertDictEqual(conductor.get_task_initial_context('task3', 0), expected_task_init_ctx)
        expected_task_ctx = dx.merge_dicts(expected_task_ctx, {'var3': True})
        expected_txsn_ctx = {'task4__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task4', expected_task_ctx)

        # Conduct task4 and check final workflow state.
        self.forward_task_states(conductor, 'task4', [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check workflow state and context.
        expected_term_ctx = copy.deepcopy(expected_task_ctx)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)

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

        # Conduct task1 and task2 then check context and next tasks.
        self.forward_task_states(conductor, 'task1', [states.RUNNING])
        self.forward_task_states(conductor, 'task2', [states.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), {})

        # Succeed task1 and check context.
        self.forward_task_states(conductor, 'task1', [states.SUCCEEDED])
        expected_init_ctx = dict()
        expected_txsn_ctx = {'task3__t0': expected_init_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, has_next_task=False)

        # Conduct task2 and check next tasks and context.
        self.forward_task_states(conductor, 'task2', [states.SUCCEEDED])
        expected_txsn_ctx = {'task3__t0': expected_init_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task2', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task3', expected_init_ctx)

        # Conduct task3 and check merged context.
        self.forward_task_states(conductor, 'task3', [states.RUNNING, states.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_initial_context('task3', 0), expected_init_ctx)
        expected_txsn_ctx = {'task4__t0': expected_init_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task4', expected_init_ctx)

        # Conduct task4 and check final workflow state.
        self.forward_task_states(conductor, 'task4', [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check workflow state and context.
        expected_term_ctx = expected_init_ctx
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)

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

        # Conduct task1 and task2 then check context and next tasks.
        self.forward_task_states(conductor, 'task1', [states.RUNNING])
        self.forward_task_states(conductor, 'task2', [states.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), {})

        # Succeed task1 and check context.
        self.forward_task_states(conductor, 'task1', [states.SUCCEEDED])
        expected_task_ctx = copy.deepcopy(inputs)
        expected_txsn_ctx = {'task3__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, has_next_task=False)

        # Conduct task2 and check next tasks and context.
        self.forward_task_states(conductor, 'task2', [states.SUCCEEDED])
        expected_txsn_ctx = {'task3__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task2', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task3', expected_task_ctx)

        # Conduct task3 and check merged context.
        self.forward_task_states(conductor, 'task3', [states.RUNNING, states.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_initial_context('task3', 0), expected_task_ctx)
        expected_txsn_ctx = {'task4__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task4', expected_task_ctx)

        # Conduct task4 and check final workflow state.
        self.forward_task_states(conductor, 'task4', [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check workflow state and context.
        expected_term_ctx = copy.deepcopy(expected_task_ctx)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)

    def test_parallel(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - var1: abc
          - var2: 234

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

        output:
          - var1: <% ctx().var1 %>
          - var2: <% ctx().var2 %>
        """

        expected_output = {
            'var1': 'xyz',
            'var2': 123
        }

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Conduct task1 and task3 and check context.
        self.forward_task_states(conductor, 'task1', [states.RUNNING])
        self.forward_task_states(conductor, 'task3', [states.RUNNING])

        self.forward_task_states(conductor, 'task1', [states.SUCCEEDED])
        expected_t1_ctx = {'var1': 'xyz', 'var2': 234}
        expected_txsn_ctx = {'task2__t0': expected_t1_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), expected_txsn_ctx)

        task_ids = ['task2']
        task_ctxs = [expected_t1_ctx]
        task_routes = [0]
        self.assert_next_tasks(conductor, task_ids, task_ctxs, task_routes)

        self.forward_task_states(conductor, 'task3', [states.SUCCEEDED])
        expected_t3_ctx = {'var1': 'abc', 'var2': 123}
        expected_txsn_ctx = {'task4__t0': expected_t3_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)

        task_ids = ['task2', 'task4']
        task_ctxs = [expected_t1_ctx, expected_t3_ctx]
        task_routes = [0] * 2
        self.assert_next_tasks(conductor, task_ids, task_ctxs, task_routes)

        # Conduct task2 and task4 and check context.
        self.forward_task_states(conductor, 'task2', [states.RUNNING])
        self.forward_task_states(conductor, 'task4', [states.RUNNING])

        self.forward_task_states(conductor, 'task2', [states.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts('task2', 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        self.forward_task_states(conductor, 'task4', [states.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts('task4', 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.flow.get_terminal_tasks()
        actual_term_tasks = [t['id'] for t in term_tasks]
        expected_term_tasks = ['task2', 'task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Check workflow state and context.
        expected_term_ctx = {'var1': 'xyz', 'var2': 123}
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
