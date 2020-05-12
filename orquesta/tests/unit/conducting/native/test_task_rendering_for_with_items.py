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


class WorkflowConductorWithItemsTaskRenderingTest(test_base.WorkflowConductorTest):
    def test_bad_item_key(self):
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
            with: x in <% ctx(xs) %>
            action: core.echo message=<% item(y) %>
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression '<% item(y) %>'. "
                    'ExpressionEvaluationException: Item does not have key "y".'
                ),
                "task_id": "task1",
                "route": 0,
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        tasks = conductor.get_next_tasks()

        self.assertListEqual(tasks, [])
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_bad_item_type(self):
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
            action: core.echo message=<% item(x) %>
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression '<% item(x) %>'. "
                    "ExpressionEvaluationException: Item is not type of collections.Mapping."
                ),
                "task_id": "task1",
                "route": 0,
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        tasks = conductor.get_next_tasks()

        self.assertListEqual(tasks, [])
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_bad_items_type(self):
        wf_def = """
        version: 1.0

        vars:
          - xs: fee fi fo fum

        tasks:
          task1:
            with: x in <% ctx(xs) %>
            action: core.echo message=<% item(y) %>
        """

        expected_errors = [
            {
                "type": "error",
                "message": 'TypeError: The value of "<% ctx(xs) %>" is not type of list.',
                "task_id": "task1",
                "route": 0,
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)
        tasks = conductor.get_next_tasks()

        self.assertListEqual(tasks, [])
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_start_task_rendering(self):
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
            with: x in <% ctx(xs) %>
            action: core.echo message=<% item(x) %>
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        task_route = 0
        next_task_name = "task1"
        next_task_ctx = {"xs": ["fee", "fi", "fo", "fum"]}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {"action": "core.echo", "input": {"message": "fee"}, "item_id": 0},
            {"action": "core.echo", "input": {"message": "fi"}, "item_id": 1},
            {"action": "core.echo", "input": {"message": "fo"}, "item_id": 2},
            {"action": "core.echo", "input": {"message": "fum"}, "item_id": 3},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
            items_count=len(next_task_ctx["xs"]),
            items_concurrency=None,
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

    def test_next_task_rendering(self):
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
            action: core.noop
            next:
              - do: task2
          task2:
            with: x in <% ctx(xs) %>
            action: core.echo message=<% item(x) %>
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Process start task.
        task_route = 0
        next_task_name = "task1"
        next_task_ctx = {"xs": ["fee", "fi", "fo", "fum"]}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [{"action": "core.noop", "input": None}]

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        status_changes = [statuses.RUNNING, statuses.SUCCEEDED]
        self.forward_task_statuses(conductor, next_task_name, status_changes)

        # Process next task.
        next_task_name = "task2"
        next_task_ctx = {"xs": ["fee", "fi", "fo", "fum"]}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {"action": "core.echo", "input": {"message": "fee"}, "item_id": 0},
            {"action": "core.echo", "input": {"message": "fi"}, "item_id": 1},
            {"action": "core.echo", "input": {"message": "fo"}, "item_id": 2},
            {"action": "core.echo", "input": {"message": "fum"}, "item_id": 3},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
            items_count=len(next_task_ctx["xs"]),
            items_concurrency=None,
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

    def test_basic_list_rendering(self):
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

        task_route = 0
        next_task_name = "task1"
        next_task_ctx = {"xs": ["fee", "fi", "fo", "fum"]}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {"action": "core.echo", "input": {"message": "fee"}, "item_id": 0},
            {"action": "core.echo", "input": {"message": "fi"}, "item_id": 1},
            {"action": "core.echo", "input": {"message": "fo"}, "item_id": 2},
            {"action": "core.echo", "input": {"message": "fum"}, "item_id": 3},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
            items_count=len(next_task_ctx["xs"]),
            items_concurrency=None,
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

    def test_basic_list_rendering_var_w_in(self):
        wf_def = """
        version: 1.0

        vars:
          - domains:
              - fee
              - fi
              - fo
              - fum

        tasks:
          task1:
            with: <% ctx(domains) %>
            action: core.echo message=<% item() %>
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        task_route = 0
        next_task_name = "task1"
        next_task_ctx = {"domains": ["fee", "fi", "fo", "fum"]}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {"action": "core.echo", "input": {"message": "fee"}, "item_id": 0},
            {"action": "core.echo", "input": {"message": "fi"}, "item_id": 1},
            {"action": "core.echo", "input": {"message": "fo"}, "item_id": 2},
            {"action": "core.echo", "input": {"message": "fum"}, "item_id": 3},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
            items_count=len(next_task_ctx["domains"]),
            items_concurrency=None,
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

    def test_multiple_lists_rendering(self):
        wf_def = """
        version: 1.0

        vars:
          - xs:
              - foo
              - fu
              - marco
          - ys:
              - bar
              - bar
              - polo

        tasks:
          task1:
            with: x, y in <% zip(ctx(xs), ctx(ys)) %>
            action: core.echo message=<% item(x) + item(y) %>
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        task_route = 0
        next_task_name = "task1"
        next_task_ctx = {"xs": ["foo", "fu", "marco"], "ys": ["bar", "bar", "polo"]}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {"action": "core.echo", "input": {"message": "foobar"}, "item_id": 0},
            {"action": "core.echo", "input": {"message": "fubar"}, "item_id": 1},
            {"action": "core.echo", "input": {"message": "marcopolo"}, "item_id": 2},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
            items_count=len(next_task_ctx["xs"]),
            items_concurrency=None,
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)
