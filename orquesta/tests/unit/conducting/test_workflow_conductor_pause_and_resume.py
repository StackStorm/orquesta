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


class WorkflowConductorPauseResumeTest(base.WorkflowConductorTest):

    def test_pause_and_resume_from_workflow(self):
        wf_def = """
        version: 1.0
        description: A basic branching workflow.
        tasks:
          # branch 1
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
          # branch 2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
          # adjoining branch
          task3:
            join: all
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Run task1 and task2.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.RUNNING))
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_state(states.PAUSING)

        # Complete task1 only. The workflow should still be pausing
        # because task2 is still running.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)

        # Complete task2. When task2 completes, the workflow should be paused
        # because there is no task in active state.
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume the workflow, task3 should be staged, and complete task3.
        conductor.request_workflow_state(states.RESUMING)
        expected_task = self.format_task_item('task3', {}, conductor.spec.tasks.get_task('task3'))
        self.assert_task_list(conductor.get_next_tasks('task2'), [expected_task])
        conductor.update_task_flow('task3', events.ActionExecutionEvent(states.RUNNING))
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        conductor.update_task_flow('task3', events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_pause_and_resume_from_branches(self):
        wf_def = """
        version: 1.0
        description: A basic branching workflow.
        tasks:
          # branch 1
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
          # branch 2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
          # adjoining branch
          task3:
            join: all
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Run task1 and task2.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.RUNNING))
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Pause task1 and task2.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.PAUSED))
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.PAUSED))
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume and complete task1 only. Once task1 completes, the workflow
        # should pause again because there is no active task.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.RUNNING))
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume and complete task2. When task2 completes, the workflow
        # should stay running because task3 is now staged and ready.
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.RUNNING))
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        expected_task = self.format_task_item('task3', {}, conductor.spec.tasks.get_task('task3'))
        self.assert_task_list(conductor.get_next_tasks('task2'), [expected_task])

        # Complete task3.
        conductor.update_task_flow('task3', events.ActionExecutionEvent(states.RUNNING))
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        conductor.update_task_flow('task3', events.ActionExecutionEvent(states.SUCCEEDED))
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
