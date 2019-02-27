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
        task_route = 0
        task_name = 'task1'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        expected_task_ctx = {'loop': True}
        expected_txsn_ctx = {'%s__t0' % next_task_name: {'srcs': [], 'value': expected_task_ctx}}
        actual_txsn_ctx = conductor.get_task_transition_contexts(task_name, task_route)
        self.assertDictEqual(actual_txsn_ctx, expected_txsn_ctx)
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        # Conduct task2 and check next tasks and context.
        task_name = 'task2'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        expected_task_ctx = {'loop': False}
        expected_txsn_ctx = {'%s__t0' % next_task_name: {'srcs': [1], 'value': expected_task_ctx}}
        actual_txsn_ctx = conductor.get_task_transition_contexts(task_name, task_route)
        self.assertDictEqual(actual_txsn_ctx, expected_txsn_ctx)
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        # Conduct task2 and check next tasks and context.
        task_name = 'task2'
        next_task_name = 'task2'
        self.forward_task_states(conductor, task_name, [states.RUNNING, states.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check workflow state and context.
        expected_term_ctx = {'loop': False}
        expected_term_ctx_entry = {'src': [2], 'term': True, 'value': expected_term_ctx}
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx_entry)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
