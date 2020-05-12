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


class WorkflowConductorWithItemsTest(test_base.WorkflowConductorWithItemsTest):
    def assert_workflow_with_single_item(self, ac_ex_status, tk_ex_status, wf_ex_status):
        wf_def = """
        version: 1.0

        vars:
          - xs:
              - fee

        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = "task1"
        task_ctx = {"xs": ["fee"]}

        task_action_specs = [
            {"action": "core.echo", "input": {"message": "fee"}, "item_id": 0},
        ]

        # Verify the set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs,
            items_count=len(task_ctx["xs"]),
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the item to running status.
        self.forward_task_item_statuses(conductor, task_name, 0, [statuses.RUNNING])

        # Assert that the task is running.
        actual_task_status = conductor.workflow_state.get_task(task_name, task_route)["status"]
        self.assertEqual(actual_task_status, statuses.RUNNING)

        # Change status for the item.
        result = task_ctx["xs"][0]
        status_changes = [ac_ex_status]
        self.forward_task_item_statuses(conductor, task_name, 0, status_changes, result=result)

        # Assert task and workflow status.
        actual_task_status = conductor.workflow_state.get_task(task_name, task_route)["status"]
        self.assertEqual(actual_task_status, tk_ex_status)
        self.assertEqual(conductor.get_workflow_status(), wf_ex_status)

    def test_workflow_with_single_item(self):
        test_cases = [
            (statuses.RUNNING, statuses.RUNNING, statuses.RUNNING),
            (statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED),
            (statuses.PENDING, statuses.PAUSED, statuses.PAUSED),
            (statuses.PAUSING, statuses.PAUSING, statuses.RUNNING),
            (statuses.RESUMING, statuses.RUNNING, statuses.RUNNING),
            (statuses.PAUSED, statuses.PAUSED, statuses.PAUSED),
            (statuses.CANCELING, statuses.CANCELING, statuses.CANCELING),
            (statuses.CANCELED, statuses.CANCELED, statuses.CANCELED),
            (statuses.FAILED, statuses.FAILED, statuses.FAILED),
            (statuses.EXPIRED, statuses.FAILED, statuses.FAILED),
            (statuses.ABANDONED, statuses.FAILED, statuses.FAILED),
        ]

        for ac_ex_status, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_single_item(ac_ex_status, tk_ex_status, wf_ex_status)

    def assert_workflow_with_items(self, ac_ex_statuses, tk_ex_status, wf_ex_status):
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
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = "task1"
        task_ctx = {"xs": ["fee", "fi", "fo", "fum"]}

        task_action_specs = [
            {"action": "core.echo", "input": {"message": "fee"}, "item_id": 0},
            {"action": "core.echo", "input": {"message": "fi"}, "item_id": 1},
            {"action": "core.echo", "input": {"message": "fo"}, "item_id": 2},
            {"action": "core.echo", "input": {"message": "fum"}, "item_id": 3},
        ]

        # Verify the set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs,
            items_count=len(task_ctx["xs"]),
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0, len(ac_ex_statuses)):
            self.forward_task_item_statuses(conductor, task_name, i, [statuses.RUNNING])

        # Assert that the task is running.
        actual_task_status = conductor.workflow_state.get_task(task_name, task_route)["status"]
        self.assertEqual(actual_task_status, statuses.RUNNING)

        # Change status for all but one item.
        for i in range(0, len(ac_ex_statuses) - 1):
            result = task_ctx["xs"][i]
            status_changes = [ac_ex_statuses[i]]
            self.forward_task_item_statuses(conductor, task_name, i, status_changes, result=result)
            actual_task_status = conductor.workflow_state.get_task(task_name, task_route)["status"]

        # Change status for the last item.
        i = len(ac_ex_statuses) - 1
        result = task_ctx["xs"][i]
        status_changes = [ac_ex_statuses[i]]
        self.forward_task_item_statuses(conductor, task_name, i, status_changes, result=result)

        # Assert task and workflow status.
        actual_task_status = conductor.workflow_state.get_task(task_name, task_route)["status"]
        self.assertEqual(actual_task_status, tk_ex_status)
        self.assertEqual(conductor.get_workflow_status(), wf_ex_status)

    def test_workflow_with_items_action_running(self):
        test_cases = [
            # ACTION_RUNNING (rest of the list incomplete)
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RUNNING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RUNNING (not all items processed, rest of the list completed)
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RUNNING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RUNNING (rest of the list completed)
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RUNNING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RUNNING (has item that is paused)
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RUNNING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_RUNNING (has item that is canceled)
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RUNNING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_RUNNING (has item that failed)
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RUNNING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RUNNING (has item that is expired)
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RUNNING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RUNNING (has item that is abandoned)
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RUNNING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_pausing(self):
        test_cases = [
            # ACTION_PAUSING (rest of the list incomplete)
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_PAUSING (not all items processed, rest of the list completed)
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_PAUSING (rest of the list completed)
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_PAUSING (has item that is paused)
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_PAUSING (has item that is canceled)
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_PAUSING (has item that failed)
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_PAUSING (has item that is expired)
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_PAUSING (has item that is abandoned)
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_resuming(self):
        test_cases = [
            # ACTION_RESUMING (rest of the list incomplete)
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RESUMING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RESUMING (not all items processed, rest of the list completed)
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_RESUMING (rest of the list completed)
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RESUMING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RESUMING (has item that is paused)
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RESUMING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_RESUMING (has item that is canceled)
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RESUMING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_RESUMING (has item that failed)
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RESUMING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RESUMING (has item that is expired)
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RESUMING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_RESUMING (has item that is abandoned)
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.RESUMING],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_canceling(self):
        test_cases = [
            # ACTION_CANCELING (rest of the list incomplete)
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_CANCELING (not all items processed, rest of the list completed)
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_CANCELING (rest of the list completed)
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_CANCELING (has item that is paused)
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_CANCELING (has item that is canceled)
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_CANCELING (has item that failed)
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_CANCELING (has item that is expired)
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_CANCELING (has item that is abandoned)
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELING],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_pending(self):
        test_cases = [
            # ACTION_PENDING_TASK_ACTIVE_ITEMS_INCOMPLETE
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PENDING],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PENDING],
                statuses.PAUSED,
                statuses.PAUSED,
            ),
            # ACTION_PENDING_TASK_DORMANT_ITEMS_COMPLETED
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PENDING],
                statuses.PAUSED,
                statuses.PAUSED,
            ),
            # ACTION_PENDING_TASK_DORMANT_ITEMS_PAUSED
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PENDING],
                statuses.PAUSED,
                statuses.PAUSED,
            ),
            # ACTION_PENDING_TASK_DORMANT_ITEMS_CANCELED
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PENDING],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_PENDING_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PENDING],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_PENDING_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PENDING],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_PENDING_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PENDING],
                statuses.FAILED,
                statuses.FAILED,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_paused(self):
        test_cases = [
            # ACTION_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSED],
                statuses.PAUSING,
                statuses.RUNNING,
            ),
            # ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSED],
                statuses.PAUSED,
                statuses.PAUSED,
            ),
            # ACTION_PAUSED_TASK_DORMANT_ITEMS_COMPLETED
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSED],
                statuses.PAUSED,
                statuses.PAUSED,
            ),
            # ACTION_PAUSED_TASK_DORMANT_ITEMS_PAUSED
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSED],
                statuses.PAUSED,
                statuses.PAUSED,
            ),
            # ACTION_PAUSED_TASK_DORMANT_ITEMS_CANCELED
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_PAUSED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_PAUSED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_PAUSED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.PAUSED],
                statuses.FAILED,
                statuses.FAILED,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_succeeded(self):
        test_cases = [
            # ACTION_SUCCEEDED_TASK_ACTIVE_ITEMS_INCOMPLETE
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED],
                statuses.SUCCEEDED,
                statuses.SUCCEEDED,
            ),
            # ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED],
                statuses.PAUSED,
                statuses.PAUSED,
            ),
            # ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_CANCELED
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED],
                statuses.FAILED,
                statuses.FAILED,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_failed(self):
        test_cases = [
            # ACTION_FAILED_TASK_ACTIVE_ITEMS_INCOMPLETE
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.FAILED],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_FAILED_TASK_DORMANT_ITEMS_INCOMPLETE
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.FAILED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_FAILED_TASK_DORMANT_ITEMS_COMPLETED
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.FAILED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_FAILED_TASK_DORMANT_ITEMS_PAUSED
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.FAILED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_FAILED_TASK_DORMANT_ITEMS_CANCELED
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.FAILED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_FAILED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.FAILED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_FAILED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.FAILED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_FAILED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.FAILED],
                statuses.FAILED,
                statuses.FAILED,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_expired(self):
        test_cases = [
            # ACTION_EXPIRED_TASK_ACTIVE_ITEMS_INCOMPLETE
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.EXPIRED],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_EXPIRED_TASK_DORMANT_ITEMS_INCOMPLETE
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.EXPIRED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_EXPIRED_TASK_DORMANT_ITEMS_COMPLETED
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.EXPIRED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_EXPIRED_TASK_DORMANT_ITEMS_PAUSED
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.EXPIRED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_EXPIRED_TASK_DORMANT_ITEMS_CANCELED
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.EXPIRED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_EXPIRED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.EXPIRED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_EXPIRED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.EXPIRED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_EXPIRED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.EXPIRED],
                statuses.FAILED,
                statuses.FAILED,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_abandoned(self):
        test_cases = [
            # ACTION_ABANDONED_TASK_ACTIVE_ITEMS_INCOMPLETE
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.ABANDONED],
                statuses.RUNNING,
                statuses.RUNNING,
            ),
            # ACTION_ABANDONED_TASK_DORMANT_ITEMS_INCOMPLETE
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.ABANDONED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_ABANDONED_TASK_DORMANT_ITEMS_COMPLETED
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.ABANDONED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_ABANDONED_TASK_DORMANT_ITEMS_PAUSED
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.ABANDONED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_ABANDONED_TASK_DORMANT_ITEMS_CANCELED
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.ABANDONED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_ABANDONED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.ABANDONED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_ABANDONED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.ABANDONED],
                statuses.FAILED,
                statuses.FAILED,
            ),
            # ACTION_ABANDONED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.ABANDONED],
                statuses.FAILED,
                statuses.FAILED,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)

    def test_workflow_with_items_action_canceled(self):
        test_cases = [
            # ACTION_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE
            (
                [statuses.RUNNING, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELED],
                statuses.CANCELING,
                statuses.CANCELING,
            ),
            # ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_CANCELED_TASK_DORMANT_ITEMS_COMPLETED
            (
                [statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_CANCELED_TASK_DORMANT_ITEMS_PAUSED
            (
                [statuses.PAUSED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_CANCELED_TASK_DORMANT_ITEMS_CANCELED
            (
                [statuses.CANCELED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_CANCELED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.FAILED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_CANCELED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.EXPIRED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
            # ACTION_CANCELED_TASK_DORMANT_ITEMS_FAILED
            (
                [statuses.ABANDONED, statuses.SUCCEEDED, statuses.SUCCEEDED, statuses.CANCELED],
                statuses.CANCELED,
                statuses.CANCELED,
            ),
        ]

        for ac_ex_statuses, tk_ex_status, wf_ex_status in test_cases:
            self.assert_workflow_with_items(ac_ex_statuses, tk_ex_status, wf_ex_status)
