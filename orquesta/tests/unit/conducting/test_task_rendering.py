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


class WorkflowConductorTaskRenderingTest(test_base.WorkflowConductorTest):
    def test_basic_rendering(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        input:
          - action_name
          - action_input

        tasks:
          task1:
            action: <% ctx().action_name %>
            input: <% ctx().action_input %>
            next:
              - publish: message=<% result() %>
                do: task2
          task2:
            action: core.echo message=<% ctx().message %>
            next:
              - do: task3
          task3:
            action: core.echo
            input:
              message: <% ctx().message %>
        """

        # Instantiate workflow spec.
        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Instantiate conductor
        action_name = "core.echo"
        action_input = {"message": "All your test_base are belong to us!"}
        inputs = {"action_name": action_name, "action_input": action_input}
        conductor = conducting.WorkflowConductor(spec, inputs=inputs)
        conductor.request_workflow_status(statuses.RUNNING)

        # Test that the action and input are rendered entirely from context.
        task_route = 0
        next_task_name = "task1"
        next_task_ctx = inputs
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        next_task_action_specs = [{"action": action_name, "input": action_input}]

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
        )

        expected_tasks = [expected_task]

        self.assert_task_list(conductor, conductor.get_next_tasks(), expected_tasks)

        # Test that the inline parameter in action is rendered.
        task_name = "task1"
        next_task_name = "task2"
        mock_result = action_input["message"]

        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED], result=mock_result)

        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        next_task_action_specs = [{"action": "core.echo", "input": {"message": mock_result}}]

        next_task_ctx = {"message": mock_result}
        next_task_ctx.update(inputs)

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
        )

        expected_tasks = [expected_task]

        self.assert_task_list(conductor, conductor.get_next_tasks(), expected_tasks)

        # Test that the individual expression in input is rendered.
        task_name = "task2"
        next_task_name = "task3"
        mock_result = action_input["message"]

        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED], result=mock_result)

        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        next_task_action_specs = [{"action": "core.echo", "input": {"message": mock_result}}]

        next_task_ctx = {"message": mock_result}
        next_task_ctx.update(inputs)

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
        )

        expected_tasks = [expected_task]

        self.assert_task_list(conductor, conductor.get_next_tasks(), expected_tasks)

    def test_with_items_rendering(self):
        wf_def = """
        version: 1.0

        input:
          - xs
          - concurrency

        tasks:
          task1:
            with:
              items: <% ctx(xs) %>
              concurrency: <% ctx(concurrency) %>
            action: core.echo message=<% item() %>
            next:
              - publish:
                  - items: <% result() %>

        output:
          - items: <% ctx(items) %>
        """

        # Instantiate workflow spec.
        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Instantiate conductor
        inputs = {"xs": ["fee", "fi", "fo", "fum"], "concurrency": 2}
        conductor = conducting.WorkflowConductor(spec, inputs=inputs)
        conductor.request_workflow_status(statuses.RUNNING)

        # Test that the items and context of a task is rendered from context.
        task_route = 0
        next_task_name = "task1"
        next_task_ctx = inputs
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
            actions=next_task_action_specs[0 : inputs["concurrency"]],
            items_count=len(inputs["xs"]),
            items_concurrency=inputs["concurrency"],
        )

        expected_tasks = [expected_task]

        self.assert_task_list(conductor, conductor.get_next_tasks(), expected_tasks)

    def test_task_delay_rendering(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        vars:
          - delay: 180

        tasks:
          task1:
            delay: <% ctx().delay %>
            action: core.noop
        """

        # Instantiate workflow spec.
        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Instantiate conductor
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Ensure the task delay is rendered correctly.
        task_route = 0
        next_task_name = "task1"
        next_task_ctx = {"delay": 180}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        next_task_action_specs = [{"action": "core.noop", "input": None}]

        expected_task = self.format_task_item(
            next_task_name,
            task_route,
            next_task_ctx,
            next_task_spec,
            actions=next_task_action_specs,
            delay=next_task_ctx["delay"],
        )

        expected_tasks = [expected_task]

        self.assert_task_list(conductor, conductor.get_next_tasks(), expected_tasks)

    def test_task_delay_rendering_bad_type(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        vars:
          - delay: foobar

        tasks:
          task1:
            delay: <% ctx().delay %>
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": "TypeError: The value of task delay is not type of integer.",
                "task_id": "task1",
                "route": 0,
            }
        ]

        # Instantiate workflow spec.
        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        # Instantiate conductor
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Assert failed status and errors.
        self.assert_next_task(conductor, has_next_task=False)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)
