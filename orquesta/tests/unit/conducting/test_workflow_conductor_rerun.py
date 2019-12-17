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
from orquesta import exceptions as exc
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base
from orquesta.utils import dictionary as dict_util


PARALLEL_WF_DEF = """
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

JOIN_WF_DEF = """
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

  # joining branch
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

REMEDIATED_WF_DEF = """
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
      - publish:
          - var2: 123
        do: task4
  task4:
    action: core.noop

output:
  - var1: <% ctx().var1 %>
  - var2: <% ctx().var2 %>
"""


class WorkflowConductorRerunTest(test_base.WorkflowConductorTest):

    def pre_rerun_workflow(self, wf_def):
        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Run the workflow.
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        return conductor

    def assert_workflow_rerun_request(self, conductor, rerun_tasks):
        prev_num_tasks = len(conductor.workflow_state.get_tasks())
        prev_num_staged_tasks = len(conductor.workflow_state.get_staged_tasks())

        options = dict()
        options['tasks'] = rerun_tasks
        conductor.request_workflow_rerun(options)

        # Assert the workflow status.
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)

        # Assert the number of tasks and staged tasks.
        tasks = conductor.workflow_state.get_tasks()
        self.assertEqual(len(tasks), prev_num_tasks + len(rerun_tasks))

        staged_tasks = conductor.workflow_state.get_staged_tasks()
        self.assertEqual(len(staged_tasks), prev_num_staged_tasks + len(rerun_tasks))

        # Assert the rerun tasks are staged and ready.
        for task in rerun_tasks:
            matched = filter(lambda x: x['id'] == task and x.get('ready', False), staged_tasks)
            self.assertEqual(len(list(matched)), 1)

        # Assert the workflow status.
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)

    def test_parallel_rerun_with_first_task(self):
        expected_output = {
            'var1': 'xyz',
            'var2': 123
        }

        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(PARALLEL_WF_DEF)

        # 2. Fail task1
        # Conduct tasks.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING])

        self.forward_task_statuses(conductor, 'task1', [statuses.FAILED])
        self.forward_task_statuses(conductor, 'task3', [statuses.SUCCEEDED])

        expected_t3_ctx = {'var1': 'abc', 'var2': 123}
        expected_txsn_ctx = {'task4__t0': expected_t3_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)

        # log the error
        error = 'test parallel rerun with first task'
        conductor.log_error(error, 'task1')

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task1', 'task3']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 3. Start rerun workflow
        options = dict()
        options['tasks'] = ['task1']
        conductor.request_workflow_rerun(options)

        # Conduct task4 and check context.
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task4', [statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts('task4', 0), {})

        # Conduct task1 and check context.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])

        expected_t1_ctx = {'var1': 'xyz', 'var2': 234}
        expected_txsn_ctx = {'task2__t0': expected_t1_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), expected_txsn_ctx)

        task_ids = ['task2']
        task_ctxs = [expected_t1_ctx]
        task_routes = [0]
        self.assert_next_tasks(conductor, task_ids, task_ctxs, task_routes)

        # Conduct task2 and check context.
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task2', [statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts('task2', 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2', 'task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        expected_term_ctx = {'var1': 'xyz', 'var2': 123}
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
        self.assertEqual(len(conductor.errors), 0)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_parallel_rerun_with_last_task(self):
        expected_output = {'var1': 'xyz', 'var2': 123}

        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(PARALLEL_WF_DEF)

        # 2. Fail task4
        # Conduct tasks.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING])

        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task3', [statuses.SUCCEEDED])

        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING])

        self.forward_task_statuses(conductor, 'task2', [statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task4', [statuses.FAILED])

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2', 'task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 3. Start rerun workflow
        rerun_tasks = ['task4']
        self.assert_workflow_rerun_request(conductor, rerun_tasks)

        # Conduct task4 and check context.
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task4', [statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts('task4', 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2', 'task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        expected_term_ctx = {'var1': 'xyz', 'var2': 123}
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
        self.assertEqual(len(conductor.errors), 0)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_parallel_rerun_with_more_than_one_tasks(self):
        expected_output = {
            'var1': 'xyz',
            'var2': 123
        }

        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(PARALLEL_WF_DEF)

        # 2. Fail task2 and task3
        # Conduct tasks1 and task3.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING])

        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task3', [statuses.FAILED])

        # Conduct task2.
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task2', [statuses.FAILED])

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2', 'task3']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 3. Start rerun workflow
        rerun_tasks = ['task2', 'task3']
        self.assert_workflow_rerun_request(conductor, rerun_tasks)

        # Conduct task2, task3 and check context.
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task2', [statuses.SUCCEEDED])

        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task3', [statuses.SUCCEEDED])

        expected_t1_ctx = {'var1': 'abc', 'var2': 123}
        expected_txsn_ctx = {'task4__t0': expected_t1_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)

        task_id = 'task4'
        task_routes = 0
        self.assert_next_task(conductor, task_id, expected_t1_ctx, task_routes, has_next_task=True)

        # Conduct task4 and check context.
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task4', [statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts('task4', 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2', 'task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        expected_term_ctx = {'var1': 'xyz', 'var2': 123}
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
        self.assertEqual(len(conductor.errors), 0)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_join_rerun_with_first_task(self):
        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(JOIN_WF_DEF)

        # 2. Fail task1
        # Conduct tasks1 and task2.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING])

        self.forward_task_statuses(conductor, 'task1', [statuses.FAILED])
        self.forward_task_statuses(conductor, 'task2', [statuses.SUCCEEDED])

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task1', 'task2']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 3. Start rerun workflow
        rerun_tasks = ['task1']
        self.assert_workflow_rerun_request(conductor, rerun_tasks)

        # Conduct task1 and check context.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])

        expected_task_ctx = {'var1': 'xyz', 'var2': 123}
        expected_txsn_ctx = {'task3__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task3', expected_task_ctx)

        # Conduct task3 and check merged context.
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING, statuses.SUCCEEDED])
        expected_task_init_ctx = expected_task_ctx
        self.assertDictEqual(conductor.get_task_initial_context('task3', 0), expected_task_init_ctx)

        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {'var3': True})
        expected_txsn_ctx = {'task4__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task4', expected_task_ctx)

        # Conduct task4 and check final workflow status.
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        expected_term_ctx = copy.deepcopy(expected_task_ctx)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertEqual(len(conductor.errors), 0)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_join_rerun_with_join_task(self):
        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(JOIN_WF_DEF)

        # 2. Fail joining task3
        # Conduct tasks1 and task2.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING])

        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task2', [statuses.SUCCEEDED])

        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task3', [statuses.FAILED])

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task3']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 3. Start rerun workflow
        rerun_tasks = ['task3']
        self.assert_workflow_rerun_request(conductor, rerun_tasks)

        # Conduct task3 and check merged context.
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING, statuses.SUCCEEDED])

        expected_task_ctx = {'var1': 'xyz', 'var2': 123}
        expected_task_init_ctx = expected_task_ctx
        self.assertDictEqual(conductor.get_task_initial_context('task3', 0), expected_task_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {'var3': True})
        expected_txsn_ctx = {'task4__t0': expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)
        self.assert_next_task(conductor, 'task4', expected_task_ctx)

        # Conduct task4 and check final workflow status.
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        expected_term_ctx = copy.deepcopy(expected_task_ctx)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertEqual(len(conductor.errors), 0)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_self_looping_rerun(self):
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

        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(wf_def)

        # 2. Fail on one of the tasks in the loop on 2nd iteration.
        # Conduct task1 and check context and that there is no next tasks yet.
        task_name = 'task1'
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        # Conduct task2 and check next tasks and context.
        task_name = 'task2'
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        # Conduct task2 and failed tasks.
        task_name = 'task2'
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.FAILED])

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 3. Start rerun workflow
        rerun_tasks = ['task2']
        self.assert_workflow_rerun_request(conductor, rerun_tasks)

        # Conduct task2 and check merged context.
        task_name = 'task2'
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of tasks and make sure only the last entry for task2 is term=True.
        tasks = [task for task in conductor.workflow_state.sequence if task['id'] == task_name]
        self.assertListEqual([task.get('term', False) for task in tasks], [False, False, True])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        expected_term_ctx = {'loop': False}
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2']
        self.assertEqual(len(conductor.errors), 0)
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

    def test_rerun_task_individually_for_workflow_with_many_tasks_failure(self):
        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(PARALLEL_WF_DEF)

        # 2. Fail task1 and task3
        # Conduct tasks1 and task3.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING, statuses.FAILED])

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task1', 'task3']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 3. Start rerun workflow only for task1
        rerun_tasks = ['task1']
        self.assert_workflow_rerun_request(conductor, rerun_tasks)

        # Conduct task1 and check context.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])

        expected_t1_ctx = {'var1': 'xyz', 'var2': 234}
        expected_txsn_ctx = {'task2__t0': expected_t1_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), expected_txsn_ctx)

        task_ids = ['task2']
        task_ctxs = [expected_t1_ctx]
        task_routes = [0]
        self.assert_next_tasks(conductor, task_ids, task_ctxs, task_routes)

        # Conduct task2 and check context.
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts('task2', 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2', 'task3']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Check workflow errors. There should be an entry for task3.
        self.assertEqual(len(conductor.errors), 1)

        # Workflow should be in failed status because task3 has error and not rerun.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 4. Start rerun workflow only for task3
        rerun_tasks = ['task3']
        self.assert_workflow_rerun_request(conductor, rerun_tasks)

        # Conduct task1 and check context.
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING, statuses.SUCCEEDED])

        expected_t3_ctx = {'var1': 'abc', 'var2': 123}
        expected_txsn_ctx = {'task4__t0': expected_t3_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)

        task_ids = ['task4']
        task_ctxs = [expected_t3_ctx]
        task_routes = [0]
        self.assert_next_tasks(conductor, task_ids, task_ctxs, task_routes)

        # Conduct task4 and check context.
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts('task4', 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2', 'task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(len(conductor.errors), 0)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_workflow_rerun_with_unrerunnable_status(self):
        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(PARALLEL_WF_DEF)

        # 2 Start rerun with running status .
        options = dict()
        options['tasks'] = "task1"
        self.assertRaises(
            exc.InvalidWorkflowRerunStatus,
            conductor.request_workflow_rerun,
            options
        )

    def test_workflow_rerun_with_all_invalid_task_ids(self):
        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(PARALLEL_WF_DEF)

        # 2. Failed task1
        # Fail task1
        # Conduct tasks.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING, statuses.SUCCEEDED])

        # 3 Start rerun with that rerunning task ids are all invalid.
        options = dict()
        options['tasks'] = ["wrong_task_id", "task"]
        self.assertRaises(
            exc.InvalidRerunTasks,
            conductor.request_workflow_rerun,
            options
        )
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_workflow_rerun_with_valid_and_invalid_task_ids(self):
        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(PARALLEL_WF_DEF)

        # 2. Failed task1
        # Fail task1
        # Conduct tasks.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING, statuses.SUCCEEDED])

        # 3 Start rerun with valid and invalid task ids.
        options = dict()
        options['tasks'] = ["task1", "invalid_task_id1", "invalid_task_id2"]
        self.assertRaises(
            exc.InvalidRerunTasks,
            conductor.request_workflow_rerun,
            options
        )

    def test_workflow_rerun_with_remediated_task(self):
        # 1. Prepare rerun workflow
        conductor = self.pre_rerun_workflow(REMEDIATED_WF_DEF)

        # 2. Fail task1 and task3
        # Conduct tasks1 and task3.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING, statuses.FAILED])

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task1', 'task3']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # 3. Start rerun workflow only for task1
        rerun_tasks = ['task1']
        self.assert_workflow_rerun_request(conductor, rerun_tasks)

        # Conduct task1 and check context.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])

        expected_t1_ctx = {'var1': 'xyz', 'var2': 234}
        expected_txsn_ctx = {'task2__t0': expected_t1_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task1', 0), expected_txsn_ctx)

        expected_t3_ctx = {'var1': 'abc', 'var2': 123}
        expected_txsn_ctx = {'task4__t0': expected_t3_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts('task3', 0), expected_txsn_ctx)

        task_ids = ['task2', 'task4']
        task_ctxs = [expected_t1_ctx, expected_t3_ctx]
        task_routes = [0, 0]
        self.assert_next_tasks(conductor, task_ids, task_ctxs, task_routes)

        # Conduct task2 and task4.
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task4', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = sorted([t['id'] for t in term_tasks])
        expected_term_tasks = ['task2', 'task4']
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Although remediated, there should be one error from the task3 failure.
        self.assertEqual(len(conductor.errors), 1)
        self.assertEqual(len(list(filter(lambda x: x['task_id'] == 'task3', conductor.errors))), 1)

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
