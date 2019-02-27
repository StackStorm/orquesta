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
        self.forward_task_states(conductor, 'task1', [states.RUNNING])
        self.forward_task_states(conductor, 'task2', [states.RUNNING])
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_state(states.PAUSING)

        # Complete task1 only. The workflow should still be pausing
        # because task2 is still running.
        self.forward_task_states(conductor, 'task1', [states.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)

        # Complete task2. When task2 completes, the workflow should be paused
        # because there is no task in active state.
        self.forward_task_states(conductor, 'task2', [states.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume the workflow, task3 should be staged, and complete task3.
        conductor.request_workflow_state(states.RESUMING)
        self.assert_next_task(conductor, 'task3', {})
        self.forward_task_states(conductor, 'task3', [states.RUNNING, states.SUCCEEDED])
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
        self.forward_task_states(conductor, 'task1', [states.RUNNING])
        self.forward_task_states(conductor, 'task2', [states.RUNNING])
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Pause task1 and task2.
        self.forward_task_states(conductor, 'task1', [states.PAUSED])
        self.forward_task_states(conductor, 'task2', [states.PAUSED])
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume and complete task1 only. Once task1 completes, the workflow
        # should pause again because there is no active task.
        self.forward_task_states(conductor, 'task1', [states.RUNNING])
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.forward_task_states(conductor, 'task1', [states.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume and complete task2. When task2 completes, the workflow
        # should stay running because task3 is now staged and ready.
        self.forward_task_states(conductor, 'task2', [states.RUNNING])
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.forward_task_states(conductor, 'task2', [states.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)
        self.assert_next_task(conductor, 'task3', {})

        # Complete task3.
        self.forward_task_states(conductor, 'task3', [states.RUNNING, states.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
