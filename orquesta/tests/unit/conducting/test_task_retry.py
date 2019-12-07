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


class WorkflowConductorTaskRetryTest(test_base.WorkflowConductorTest):

    def test_task_state_with_retry(self):
        wf_def = """
        version: 1.0
        description: A basic workflow with a task retry.
        tasks:
          task1:
            action: core.noop
            retry:
              when: <% failed() %>
              delay: 1
              count: 3
          task2:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        task_route = 0
        task_name = 'task1'
        conductor.add_task_state(task_name, task_route)

        expected_task_state_idx = 0
        actual_task_state_idx = conductor._get_task_state_idx(task_name, task_route)
        self.assertEqual(actual_task_state_idx, expected_task_state_idx)

        expected_task_state_entry = {
            'id': task_name,
            'route': task_route,
            'ctxs': {'in': [0]},
            'next': {},
            'prev': {},
            'retry': {
                'when': '<% failed() %>',
                'delay': 1,
                'count': 3,
                'tally': 0
            }
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

    def test_task_state_with_retry_delay_and_count_expressions(self):
        wf_def = """
        version: 1.0
        description: A basic workflow with a task retry.
        vars:
          - delay_value: 30
          - count_value: 1
        tasks:
          task1:
            action: core.noop
            retry:
              when: <% failed() %>
              delay: <% ctx().delay_value %>
              count: <% ctx().count_value %>
          task2:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        task_route = 0
        task_name = 'task1'
        conductor.add_task_state(task_name, task_route)

        expected_task_state_idx = 0
        actual_task_state_idx = conductor._get_task_state_idx(task_name, task_route)
        self.assertEqual(actual_task_state_idx, expected_task_state_idx)

        expected_task_state_entry = {
            'id': task_name,
            'route': task_route,
            'ctxs': {'in': [0]},
            'next': {},
            'prev': {},
            'retry': {
                'when': '<% failed() %>',
                'delay': 30,
                'count': 1,
                'tally': 0
            }
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

    def test_task_state_with_retry_delay_bad_type(self):
        wf_def = """
        version: 1.0
        description: A basic workflow with a task retry.
        vars:
          - delay_value: abc
        tasks:
          task1:
            action: core.noop
            retry:
              when: <% failed() %>
              delay: <% ctx().delay_value %>
              count: 3
          task2:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        task_route = 0
        task_name = 'task1'

        self.assertRaisesRegexp(
            ValueError,
            'The retry delay for task "task1" is not an integer.',
            conductor.add_task_state,
            task_name,
            task_route
        )

    def test_task_state_with_retry_count_bad_type(self):
        wf_def = """
        version: 1.0
        description: A basic workflow with a task retry.
        vars:
          - count_value: abc
        tasks:
          task1:
            action: core.noop
            retry:
              when: <% failed() %>
              delay: 1
              count: <% ctx().count_value %>
          task2:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        task_route = 0
        task_name = 'task1'

        self.assertRaisesRegexp(
            ValueError,
            'The retry count for task "task1" is not an integer.',
            conductor.add_task_state,
            task_name,
            task_route
        )
