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

WF_DEF_TO_TEST_RETRYING_TASK = """
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

WF_DEF_TO_TEST_RETRYING_TASK_WITH_ITEMS = """
version: 1.0

vars:
  - xs:
    - foo

tasks:
  task1:
    with: <% ctx(xs) %>
    action: core.echo message=<% item() %>
    retry:
      when: <% failed() %>
      count: 1
"""

WF_DEF_TO_TEST_RETRYING_DUE_TO_RESULTS = """
version: 1.0

input:
  - retry_status_code

tasks:
  task1:
    action: some.action
    retry:
      when: <% failed() or result().status_code = ctx(retry_status_code) %>
      count: 1
"""

WF_DEF_TO_TEST_ABORTING_WORKFLOW = """
version: 1.0

tasks:
  task1:
    action: core.noop
    next:
      - when: <% succeeded() %>
        do:
          - task2
          - task3
  task2:
    action: core.noop
    next:
      - when: <% ctx(INVALID_VARIABLE) %>
  task3:
    action: core.noop
    retry:
      when: <% failed() %>
      count: 3
"""


class WorkflowConductorRetryTaskTest(test_base.WorkflowConductorTest):

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
              when: <% failed() %>
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
        conductor = self._prep_conductor(WF_DEF_TO_TEST_RETRYING_TASK, task_inputs)

        # Even though the task1 was failed, it would be scheduled again because of retry statement.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # This confirms whether get_next_tasks method returns object that has delay parameter that
        # indicates workflow engine to delay task execution for specified seconds in the spec.
        expected_task = self.format_task_item(
            task_id='task1',
            route=0,
            ctx=task_inputs,
            spec=conductor.spec.tasks.get_task('task1'),
            actions=[{'action': 'core.noop', 'input': None}],
            delay=task_inputs['delay'],
        )
        actual_tasks = conductor.get_next_tasks()

        self.assert_task_list(conductor, actual_tasks, [expected_task])

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
            conductor = self._prep_conductor(WF_DEF_TO_TEST_RETRYING_TASK, task_info['inputs'])

            # This checks task would finish safely and also there is error message
            self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])
            self.assertEqual(conductor.get_workflow_status(), task_info['wf_status'])
            self.assertEqual(len(conductor.errors), 0)

    def test_retrying_task_due_to_task_result_matches_with_retry_condition(self):
        conductor = self._prep_conductor(WF_DEF_TO_TEST_RETRYING_DUE_TO_RESULTS, {
            'retry_status_code': 401,
        })

        # Task will be retried even tough task status  match with retry condition because
        # additional condition, result().status_code = 401, matches with its condition.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED], results=[{
            'status_code': 401
        }])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Even through task status and results are mached with retry condition, task won't be
        # retried because retry_count is run out.
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED], results=[{
            'status_code': 401
        }])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_not_retrying_task_due_to_task_result_does_not_match_with_retry_condition(self):
        conductor = self._prep_conductor(WF_DEF_TO_TEST_RETRYING_DUE_TO_RESULTS, {
            'retry_status_code': 200,
        })

        # Task will be retried even tough task status  match with retry condition because
        # additional condition, result().status_code = 401, matches with its condition.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED], results=[{
            'status_code': 401
        }])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_retrying_task_has_items(self):
        conductor = self._prep_conductor(WF_DEF_TO_TEST_RETRYING_TASK_WITH_ITEMS)

        # Initialize task has items and check it
        expected_task = self.format_task_item(
            task_id='task1',
            route=0,
            ctx={'xs': ['foo']},
            spec=conductor.spec.tasks.get_task('task1'),
            actions=[{'action': 'core.echo', 'input': {'message': 'foo'}, 'item_id': 0}],
            items_count=1,
            delay=0,
        )
        actual_tasks = conductor.get_next_tasks()

        self.assert_task_list(conductor, actual_tasks, [expected_task])

        # Task which has items will be retried, because result status and retry condition
        # are matched.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING], [0])
        self.forward_task_statuses(conductor, 'task1', [statuses.FAILED], [0])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # When task_state runs out of retry_count, task will be finished as appropriate status
        self.forward_task_statuses(conductor, 'task1', [statuses.FAILED], [0])
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_not_retrying_task_has_items(self):
        conductor = self._prep_conductor(WF_DEF_TO_TEST_RETRYING_TASK_WITH_ITEMS)

        # Initialize task has items
        conductor.get_next_tasks()

        # Task which has items won't be retried, because result status and retry condition
        # aren't matched.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING], [0])
        self.forward_task_statuses(conductor, 'task1', [statuses.SUCCEEDED], [0])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_serialization(self):
        task_inputs = {'count': 3}

        # Initialize workflow spec and conductor
        conductor = self._prep_conductor(WF_DEF_TO_TEST_RETRYING_TASK, task_inputs)

        # Run task1 just one time
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])

        # Serialize conductor to share it between multiple processes
        serialized_data = conductor.serialize()

        # Confirms whethere task_state information is saved as expected
        self.assertEqual(len(serialized_data['state']['sequence']), 1)
        serialized_task_state = serialized_data['state']['sequence'][0]
        self.assertIsInstance(serialized_task_state, dict)
        self.assertEqual(serialized_task_state['retry_count'], task_inputs['count'] - 1)

        # Confirms whetheer task_state_etnry is restored as expected
        deserialized_conductor = conducting.WorkflowConductor.deserialize(serialized_data)
        task_state_entry = deserialized_conductor.get_task_state_entry('task1', 0)
        self.assertEqual(task_state_entry['retry_count'], task_inputs['count'] - 1)

    def test_add_task_state_with_invalid_variable_reference_in_yaql(self):
        wf_def = """
        version: 1.0

        vars:
          - VALID_VARIABLE: 10

        tasks:
          task1:
            action: core.noop
            retry:
              when: <% failed() %>
              count: <% ctx(VALID_VARIABLE) %>
              delay: <% ctx(INVALID_VARIABLE) %>
        """

        # Initialize workflow spec and conductor
        conductor = self._prep_conductor(wf_def)

        # Create task_state_entry and confirm it is initialized as expected
        task_state_entry = conductor.add_task_state('task1', 0)
        self.assertEqual(task_state_entry['retry_count'], 10)
        self.assertEqual(task_state_entry['retry_delay'], 0)

    def test_add_task_state_with_invalid_variable_reference_in_jinja(self):
        wf_def = """
        version: 1.0

        vars:
          - VALID_VARIABLE: 10

        tasks:
          task1:
            action: core.noop
            retry:
              when: <% failed() %>
              count: '{{ ctx("VALID_VARIABLE") }}'
              delay: '{{ ctx("INVALID_VARIABLE") }}'
        """

        # Initialize workflow spec and conductor
        conductor = self._prep_conductor(wf_def)

        # Create task_state_entry and confirm it is initialized as expected
        task_state_entry = conductor.add_task_state('task1', 0)
        self.assertEqual(task_state_entry['retry_count'], 10)
        self.assertEqual(task_state_entry['retry_delay'], 0)

    def test_retrying_task_when_workflow_is_aborted(self):
        # Initialize workflow spec and conductor
        conductor = self._prep_conductor(WF_DEF_TO_TEST_ABORTING_WORKFLOW)

        # After task1 is finished, both task2 and task3 would be run.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING])
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING])

        # After finishing task2, workflow would be failed because it refers invalid variable
        self.forward_task_statuses(conductor, 'task2', [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # Usually, when task3 is failed, it would be retried because it is specified to be retried
        # when it is failed in the metadata. But in this case, this task never be retried because
        # workflow has already been failed.
        self.forward_task_statuses(conductor, 'task3', [statuses.FAILED])
        self.assertEqual(conductor.get_task_state_entry('task3', 0)['status'], statuses.FAILED)

    def test_retrying_task_after_workflow_is_aborted(self):
        # Initialize workflow spec and conductor
        conductor = self._prep_conductor(WF_DEF_TO_TEST_ABORTING_WORKFLOW)

        # After finishing task2 workflow was aborted because of referring to invalid variable.
        self.forward_task_statuses(conductor, 'task1', [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, 'task2', [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # When task3 is running after borting workflow, a new task_state_entry for this task
        # would be created, but this would never be retried.
        self.forward_task_statuses(conductor, 'task3', [statuses.RUNNING, statuses.FAILED])
        self.assertEqual(conductor.get_task_state_entry('task3', 0)['status'], statuses.FAILED)
