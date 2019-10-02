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

import mock

from orquesta import conducting
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base

WF_DEF_TO_TEST_TASK_RETRY = """
version: 1.0

input:
  - count
  - delay

tasks:
  task1:
    action: core.noop
    retry:
      when: <% succeeded() %>
      count: <% ctx(count) %>
      delay: <% ctx(delay) %>
"""


class WorkflowConductorRetryTaskTest(test_base.WorkflowConductorTest):

    def setUp(self, *args, **kwargs):
        super(WorkflowConductorRetryTaskTest, self).setUp(*args, **kwargs)

        self.test_ctx = {}

    def test_retrying_task_without_delay(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            retry:
              when: '<% failed() %>'
              count: 2
        """
        spec = native_specs.WorkflowSpec(wf_def)

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Even though the task1 was failed, it would be scheduled again because of retry statement.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assert_next_task(conductor, 'task1', {})

        # Once this task was executed for times which is specified of retry.count, it will be end.
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    @mock.patch('orquesta.conducting.time.sleep')
    def test_retrying_task_with_delay(self, mock_sleep):
        # Mock processing to confirm whether task scheduing would be delay, or not.
        def side_effect_for_sleep(*args, **kwargs):
            self.test_ctx['sleep_is_called'] = True
        mock_sleep.side_effect = side_effect_for_sleep

        task_inputs = {'count': 2, 'delay': 1}

        spec = native_specs.WorkflowSpec(WF_DEF_TO_TEST_TASK_RETRY)
        conductor = conducting.WorkflowConductor(spec, inputs=task_inputs)
        conductor.request_workflow_status(statuses.RUNNING)

        # Even though the task1 was failed, it would be scheduled again because of retry statement.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assert_next_task(conductor, 'task1', task_inputs)

        # This checks that task scheduling will be delay until specified time is passed.
        self.assertTrue(self.test_ctx['sleep_is_called'])

        # This checks task would be finish up after being successful for specified times
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    @mock.patch('orquesta.conducting.time.sleep')
    def test_retrying_task_with_invalid_params(self, mock_sleep):
        # Mock processing to confirm whether task scheduing would be delay, or not.
        def side_effect_for_sleep(*args, **kwargs):
            self.test_ctx['sleep_is_called'] = True

        mock_sleep.side_effect = side_effect_for_sleep

        task_info_list = [
            {'inputs': {'count': 'INVALID_VALUE', 'delay': '0'}, 'wf_status': statuses.SUCCEEDED},
            {'inputs': {'count': '1', 'delay': 'INVALID_VALUE'}, 'wf_status': statuses.RUNNING},
        ]
        for task_info in task_info_list:
            spec = native_specs.WorkflowSpec(WF_DEF_TO_TEST_TASK_RETRY)
            conductor = conducting.WorkflowConductor(spec, inputs=task_info['inputs'])
            conductor.request_workflow_status(statuses.RUNNING)

            # This checks task would finish safely and also there is error message
            self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])
            self.assertEqual(conductor.get_workflow_status(), task_info['wf_status'])
            self.assertEqual(len(conductor.errors), 1)
            self.assertEqual(conductor.errors[0]['type'], 'error')
            self.assertIn('Invalid value was specified at retry', conductor.errors[0]['message'])
            self.assertEqual(conductor.errors[0]['task_id'], 'task1')
            self.assertEqual(conductor.errors[0]['route'], 0)

            # Task scheduling never be delay because of invalid parameter value
            self.assertNotIn('sleep_is_called', self.test_ctx)
