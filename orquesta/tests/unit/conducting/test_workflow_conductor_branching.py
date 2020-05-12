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
from orquesta.utils import dictionary as dict_util
from orquesta.utils import jsonify as json_util


class WorkflowConductorExtendedTest(test_base.WorkflowConductorTest):
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

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Conduct task1 and task2 then check context and next tasks.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assertDictEqual(conductor.get_task_transition_contexts("task1", 0), {})

        # Succeed task1 and check context.
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        expected_task_ctx = {"var1": "xyz"}
        expected_txsn_ctx = {"task3__t0": expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task1", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, has_next_task=False)

        # Conduct task2 and check next tasks and context.
        self.forward_task_statuses(conductor, "task2", [statuses.SUCCEEDED])
        expected_task_ctx = {"var1": "xyz", "var2": 123}
        expected_txsn_ctx = {"task3__t0": expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task2", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, "task3", expected_task_ctx)

        # Conduct task3 and check merged context.
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        expected_task_init_ctx = expected_task_ctx
        self.assertDictEqual(conductor.get_task_initial_context("task3", 0), expected_task_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"var3": True})
        expected_txsn_ctx = {"task4__t0": expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task3", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, "task4", expected_task_ctx)

        # Conduct task4 and check final workflow status.
        self.forward_task_statuses(conductor, "task4", [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check workflow status and context.
        expected_term_ctx = json_util.deepcopy(expected_task_ctx)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
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

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Conduct task1 and task2 then check context and next tasks.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assertDictEqual(conductor.get_task_transition_contexts("task1", 0), {})

        # Succeed task1 and check context.
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        expected_init_ctx = dict()
        expected_txsn_ctx = {"task3__t0": expected_init_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task1", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, has_next_task=False)

        # Conduct task2 and check next tasks and context.
        self.forward_task_statuses(conductor, "task2", [statuses.SUCCEEDED])
        expected_txsn_ctx = {"task3__t0": expected_init_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task2", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, "task3", expected_init_ctx)

        # Conduct task3 and check merged context.
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_initial_context("task3", 0), expected_init_ctx)
        expected_txsn_ctx = {"task4__t0": expected_init_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task3", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, "task4", expected_init_ctx)

        # Conduct task4 and check final workflow status.
        self.forward_task_statuses(conductor, "task4", [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check workflow status and context.
        expected_term_ctx = expected_init_ctx
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
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

        inputs = {"var1": "xyz"}
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec, inputs=inputs)
        conductor.request_workflow_status(statuses.RUNNING)

        # Conduct task1 and task2 then check context and next tasks.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assertDictEqual(conductor.get_task_transition_contexts("task1", 0), {})

        # Succeed task1 and check context.
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        expected_task_ctx = json_util.deepcopy(inputs)
        expected_txsn_ctx = {"task3__t0": expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task1", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, has_next_task=False)

        # Conduct task2 and check next tasks and context.
        self.forward_task_statuses(conductor, "task2", [statuses.SUCCEEDED])
        expected_txsn_ctx = {"task3__t0": expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task2", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, "task3", expected_task_ctx)

        # Conduct task3 and check merged context.
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_initial_context("task3", 0), expected_task_ctx)
        expected_txsn_ctx = {"task4__t0": expected_task_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task3", 0), expected_txsn_ctx)
        self.assert_next_task(conductor, "task4", expected_task_ctx)

        # Conduct task4 and check final workflow status.
        self.forward_task_statuses(conductor, "task4", [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check workflow status and context.
        expected_term_ctx = json_util.deepcopy(expected_task_ctx)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
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

        expected_output = {"var1": "xyz", "var2": 123}

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Conduct task1 and task3 and check context.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING])

        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        expected_t1_ctx = {"var1": "xyz", "var2": 234}
        expected_txsn_ctx = {"task2__t0": expected_t1_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task1", 0), expected_txsn_ctx)

        task_ids = ["task2"]
        task_ctxs = [expected_t1_ctx]
        task_routes = [0]
        self.assert_next_tasks(conductor, task_ids, task_ctxs, task_routes)

        self.forward_task_statuses(conductor, "task3", [statuses.SUCCEEDED])
        expected_t3_ctx = {"var1": "abc", "var2": 123}
        expected_txsn_ctx = {"task4__t0": expected_t3_ctx}
        self.assertDictEqual(conductor.get_task_transition_contexts("task3", 0), expected_txsn_ctx)

        task_ids = ["task2", "task4"]
        task_ctxs = [expected_t1_ctx, expected_t3_ctx]
        task_routes = [0] * 2
        self.assert_next_tasks(conductor, task_ids, task_ctxs, task_routes)

        # Conduct task2 and task4 and check context.
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        self.forward_task_statuses(conductor, "task4", [statuses.RUNNING])

        self.forward_task_statuses(conductor, "task2", [statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts("task2", 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        self.forward_task_statuses(conductor, "task4", [statuses.SUCCEEDED])
        self.assertDictEqual(conductor.get_task_transition_contexts("task4", 0), {})
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of terminal tasks.
        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = [t["id"] for i, t in term_tasks]
        expected_term_tasks = ["task2", "task4"]
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

        # Render workflow output and check workflow status and context.
        conductor.render_workflow_output()
        expected_term_ctx = {"var1": "xyz", "var2": 123}
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
