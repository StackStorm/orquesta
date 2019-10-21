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

    def _prep_conductor(self, wf_def, inputs={}):
        # This nitializes workflow spec and conductor
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec, inputs=inputs)
        conductor.request_workflow_status(statuses.RUNNING)

        return conductor

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
        conductor = self._prep_conductor(wf_def)

        # Even though the task1 was failed, it would be scheduled again because of retry statement.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.FAILED])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assert_next_task(conductor, 'task1', {})

        # Once this task was executed for times which is specified of retry.count, it will be end.
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_retrying_task_with_delay(self):
        task_inputs = {'count': 2, 'delay': 1}

        # Initialize conductor
        conductor = self._prep_conductor(WF_DEF_TO_TEST_TASK_RETRY, task_inputs)

        # Even though the task1 was failed, it would be scheduled again because of retry statement.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # This confirms whether get_next_tasks method returns object that has delay parameter that
        # indicates workflow engine to delay task execution for specified seconds in the spec.
        expected_tasks = self.format_task_item(
            task_id='task1',
            route=0,
            ctx=task_inputs,
            spec=conductor.spec.tasks.get_task('task1'),
            actions=[{'action': 'core.noop', 'input': None}],
            delay=task_inputs['delay'],
        )
        actual_tasks = conductor.get_next_tasks()

        self.assert_task_list(conductor, actual_tasks, [expected_tasks])

        # This checks task would be finish up after being successful for specified times
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_retrying_task_with_invalid_params(self):
        task_info_list = [
            {'inputs': {'count': 'INVALID_VALUE', 'delay': '0'}, 'wf_status': statuses.SUCCEEDED},
            {'inputs': {'count': '1', 'delay': 'INVALID_VALUE'}, 'wf_status': statuses.RUNNING},
        ]
        for task_info in task_info_list:
            conductor = self._prep_conductor(WF_DEF_TO_TEST_TASK_RETRY, task_info['inputs'])

            # This checks task would finish safely and also there is error message
            self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])
            self.assertEqual(conductor.get_workflow_status(), task_info['wf_status'])
            self.assertEqual(len(conductor.errors), 0)

    def test_serialization(self):
        task_inputs = {'count': 3}

        # Initialize workflow spec and conductor
        conductor = self._prep_conductor(WF_DEF_TO_TEST_TASK_RETRY, task_inputs)

        # Run task1 just one time
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])

        # Serialize conductor to share it between multiple processes
        serialized_data = conductor.serialize()

        # Confirms whethere task_state information is saved as expected
        self.assertEqual(len(serialized_data['state']['sequence']), 1)
        serialized_task_state = serialized_data['state']['sequence'][0]
        self.assertIsInstance(serialized_task_state, dict)
        self.assertEqual(serialized_task_state['retry_count'], task_inputs['count'] - 1)

        # Confirms whetheer task_state_etnry is restored as an instance of TaskState
        deserialized_conductor = conducting.WorkflowConductor.deserialize(serialized_data)
        task_state_entry = deserialized_conductor.get_task_state_entry('task1', 0)

        self.assertIsInstance(task_state_entry, conducting.TaskState)
        self.assertEqual(task_state_entry['retry_count'], task_inputs['count'] - 1)
        self.assertTrue(task_state_entry.is_retried())
