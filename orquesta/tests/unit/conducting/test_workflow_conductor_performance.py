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

import random
import string

from orquesta import conducting
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorStressTest(test_base.WorkflowConductorTest):
    def _prep_wf_def(self, num_tasks):
        wf_def = {"input": ["data"], "tasks": {}}

        for i in range(1, num_tasks):
            task_name = "t" + str(i)
            next_task_name = "t" + str(i + 1)
            wf_def["tasks"][task_name] = {"action": "core.noop", "next": [{"do": next_task_name}]}

        task_name = "t" + str(num_tasks)
        wf_def["tasks"][task_name] = {"action": "core.noop"}

        return wf_def

    def _prep_conductor(self, num_tasks, context=None, inputs=None, status=None):
        wf_def = self._prep_wf_def(num_tasks)
        spec = native_specs.WorkflowSpec(wf_def)

        kwargs = {
            "context": context if context is not None else None,
            "inputs": inputs if inputs is not None else None,
        }

        conductor = conducting.WorkflowConductor(spec, **kwargs)

        if status:
            conductor.request_workflow_status(status)

        return conductor

    def test_runtime_function_of_graph_size(self):
        num_tasks = 100

        conductor = self._prep_conductor(num_tasks, status=statuses.RUNNING)

        for i in range(1, num_tasks + 1):
            task_name = "t" + str(i)
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_serialization_function_of_graph_size(self):
        num_tasks = 100
        conductor = self._prep_conductor(num_tasks, status=statuses.RUNNING)
        conductor.deserialize(conductor.serialize())

    def test_serialization_function_of_data_size(self):
        data_length = 1000000
        data = "".join(random.choice(string.ascii_lowercase) for _ in range(data_length))
        self.assertEqual(len(data), data_length)
        conductor = self._prep_conductor(1, inputs={"data": data}, status=statuses.RUNNING)
        conductor.deserialize(conductor.serialize())


class WorkflowConductorWithItemsStressTest(test_base.WorkflowConductorWithItemsTest):
    def test_runtime_function_of_items_list_size(self):
        wf_def = """
        version: 1.0

        vars:
          - xs: <% range(500).select(str($)) %>

        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - publish:
                  - items: <% result() %>

        output:
          - items: <% ctx(items) %>
        """

        num_items = 500

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = "task1"
        task_ctx = {"xs": [str(i) for i in range(0, num_items)]}

        task_action_specs = [
            {"action": "core.echo", "input": {"message": i}, "item_id": int(i)}
            for i in task_ctx["xs"]
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * num_items
        expected_task_statuses = [statuses.RUNNING] * (num_items - 1) + [statuses.SUCCEEDED]
        expected_workflow_statuses = [statuses.RUNNING] * (num_items - 1) + [statuses.SUCCEEDED]

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
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        # Assert the workflow output is correct.
        conductor.render_workflow_output()
        expected_output = {"items": task_ctx["xs"]}
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
