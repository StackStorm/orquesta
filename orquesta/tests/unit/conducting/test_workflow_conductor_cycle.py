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


class WorkflowConductorExtendedTest(test_base.WorkflowConductorTest):
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

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Conduct task1 and check context and that there is no next tasks yet.
        task_route = 0
        task_name = "task1"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        expected_task_ctx = {"loop": True}
        expected_txsn_ctx = {"%s__t0" % next_task_name: expected_task_ctx}
        actual_txsn_ctx = conductor.get_task_transition_contexts(task_name, task_route)
        self.assertDictEqual(actual_txsn_ctx, expected_txsn_ctx)
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        # Conduct task2 and check next tasks and context.
        task_name = "task2"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        expected_task_ctx = {"loop": False}
        expected_txsn_ctx = {"%s__t0" % next_task_name: expected_task_ctx}
        actual_txsn_ctx = conductor.get_task_transition_contexts(task_name, task_route)
        self.assertDictEqual(actual_txsn_ctx, expected_txsn_ctx)
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        # Conduct task2 and check next tasks and context.
        task_name = "task2"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)

        # Check the list of tasks and make sure only the last entry for task2 is term=True.
        tasks = [task for task in conductor.workflow_state.sequence if task["id"] == "task2"]
        self.assertListEqual([task.get("term", False) for task in tasks], [False, True])

        # Check workflow status and context.
        expected_term_ctx = {"loop": False}
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
