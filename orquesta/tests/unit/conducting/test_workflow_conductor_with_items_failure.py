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
from orquesta import events
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorWithItemsFailureTest(test_base.WorkflowConductorWithItemsTest):
    def test_fail_one_and_only_item(self):
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

        mock_ac_ex_statuses = [statuses.FAILED]
        expected_task_statuses = [statuses.FAILED]
        expected_workflow_statuses = [statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
        )

        # Assert the task is not removed from staging. This is intentional so the with items
        # task can be rerun partially for failed items or items that hasn't been run.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_failed_item_task_dormant_other_incomplete(self):
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

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING, statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
        )

        # Assert the task is not removed from staging. This is intentional so the with items
        # task can be rerun partially for failed items or items that hasn't been run.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_failed_item_task_dormant_other_failed(self):
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

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
        )

        # Assert the task is not removed from staging. This is intentional so the with items
        # task can be rerun partially for failed items or items that hasn't been run.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_failed_item_task_active(self):
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

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.FAILED,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED,
        ]

        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        expected_workflow_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
        )

        # Assert the task is not removed from staging. This is intentional so the with items
        # task can be rerun partially for failed items or items that hasn't been run.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_failed_item_task_dormant_with_concurrency(self):
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
            with:
              items: <% ctx(xs) %>
              concurrency: 2
            action: core.echo message=<% item() %>
        """

        concurrency = 2

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

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING, statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency,
        )

        # Assert the task is not removed from staging. This is intentional so the with items
        # task can be rerun partially for failed items or items that hasn't been run.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_failed_item_task_active_with_concurrency(self):
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
            with:
              items: <% ctx(xs) %>
              concurrency: 2
            action: core.echo message=<% item() %>
        """

        concurrency = 2

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

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.FAILED,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED,
        ]

        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        expected_workflow_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency,
        )

        # Assert the task is not removed from staging. This is intentional so the with items
        # task can be rerun partially for failed items or items that hasn't been run.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_failed_item_with_task_remediation(self):
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
            next:
              - do: task2
          task2:
            action: core.noop
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

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.FAILED,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED,
        ]

        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        expected_workflow_statuses = [statuses.RUNNING] * 4

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
        )

        current_task = conductor.get_next_tasks()[0]
        current_task_id = current_task["id"]
        current_task_route = current_task["route"]
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
        conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)

        # Assert there are only two tasks executed.
        self.assertEqual(len(conductor.workflow_state.sequence), 2)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_failed_all_items_with_task_remediation(self):
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
            next:
              - do: task2
          task2:
            action: core.noop
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

        mock_ac_ex_statuses = [
            statuses.FAILED,
            statuses.FAILED,
            statuses.FAILED,
            statuses.FAILED,
        ]

        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        expected_workflow_statuses = [statuses.RUNNING] * 4

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
        )

        current_task = conductor.get_next_tasks()[0]
        current_task_id = current_task["id"]
        current_task_route = current_task["route"]
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
        conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)

        # Assert there are only two tasks executed.
        self.assertEqual(len(conductor.workflow_state.sequence), 2)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_failed_item_task_dormant_with_concurrency_and_task_remediation(self):
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
            with:
              items: <% ctx(xs) %>
              concurrency: 2
            action: core.echo message=<% item() %>
            next:
              - do: task2
          task2:
            action: core.noop
        """

        concurrency = 2

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

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING] * 4

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency,
        )

        current_task = conductor.get_next_tasks()[0]
        current_task_id = current_task["id"]
        current_task_route = current_task["route"]
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
        conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)

        # Assert there are only two tasks executed.
        self.assertEqual(len(conductor.workflow_state.sequence), 2)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_task_cycle(self):
        wf_def = """
        version: 1.0

        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum

        tasks:
          init:
            next:
              - do: task1
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% failed() %>
                do: task1
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Get pass the init task, required for bootstrapping self looping task..
        self.forward_task_statuses(conductor, "init", [statuses.RUNNING, statuses.SUCCEEDED])

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

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING] * 2

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
        )

        # Assert the task is reset in staging.
        staged_task = conductor.workflow_state.get_staged_task(task_name, task_route)
        self.assertIsNotNone(staged_task)
        self.assertNotIn("items", staged_task)

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Mock the second task execution.
        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 4
        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.SUCCEEDED]
        expected_workflow_statuses = [statuses.RUNNING] * 3 + [statuses.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx["xs"],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.workflow_state.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
