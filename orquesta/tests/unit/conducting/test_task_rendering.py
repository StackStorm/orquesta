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


class WorkflowConductorTaskRenderingTest(base.WorkflowConductorTest):

    def _prep_conductor(self, context=None, inputs=None, state=None):
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

        spec = specs.WorkflowSpec(wf_def)

        kwargs = {
            'context': context if context is not None else None,
            'inputs': inputs if inputs is not None else None
        }

        conductor = conducting.WorkflowConductor(spec, **kwargs)

        if state:
            conductor.request_workflow_state(state)

        return conductor

    def test_runtime_rendering(self):
        action_name = 'core.echo'
        action_input = {'message': 'All your base are belong to us!'}
        inputs = {'action_name': action_name, 'action_input': action_input}
        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        # Test that the action and input are rendered entirely from context.
        task_name = 'task1'
        task1_spec = conductor.spec.tasks.get_task(task_name).copy()
        task1_spec.action = action_name
        task1_spec.input = action_input
        expected = [self.format_task_item(task_name, inputs, task1_spec)]
        self.assert_task_list(conductor.get_start_tasks(), expected)

        # Test that the inline parameter in action is rendered.
        task_name = 'task1'
        next_task_name = 'task2'
        mock_result = action_input['message']
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result=mock_result)
        conductor.update_task_flow(task_name, ac_ex_event)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        next_task_spec.action = 'core.echo'
        next_task_spec.input = {'message': mock_result}
        expected_ctx_value = {'message': mock_result}
        expected_ctx_value.update(inputs)
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)

        # Test that the individual expression in input is rendered.
        task_name = 'task2'
        next_task_name = 'task3'
        mock_result = action_input['message']
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result=mock_result)
        conductor.update_task_flow(task_name, ac_ex_event)
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)
        next_task_spec.action = 'core.echo'
        next_task_spec.input = {'message': mock_result}
        expected_ctx_value = {'message': mock_result}
        expected_ctx_value.update(inputs)
        expected_tasks = [self.format_task_item(next_task_name, expected_ctx_value, next_task_spec)]
        self.assert_task_list(conductor.get_next_tasks(task_name), expected_tasks)
