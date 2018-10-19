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
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base


class WorkflowConductorExtendedTaskTest(base.WorkflowConductorTest):

    def test_init_task_with_no_action(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            next:
              - publish: xyz=123
                do: task2
          task2:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Process task1.
        next_task_name = 'task1'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = {}
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(), expected_tasks)

        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # Process task2.
        next_task_name = 'task2'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = {'xyz': 123}
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        task_name = 'task2'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_next_task_with_no_action(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            next:
              - do: task2
          task2:
            next:
              - publish: xyz=123
                do: task3
          task3:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Process task1.
        next_task_name = 'task1'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = {}
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(), expected_tasks)

        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # Process task2.
        next_task_name = 'task2'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = {}
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        task_name = 'task2'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # Process task3.
        next_task_name = 'task3'
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        expected_ctx_value = {'xyz': 123}
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        task_name = 'task3'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
