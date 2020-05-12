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


class WorkflowConductorPauseResumeTest(test_base.WorkflowConductorTest):
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

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Run task1 and task2.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)

        # Complete task1 only. The workflow should still be pausing
        # because task2 is still running.
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

        # Complete task2. When task2 completes, the workflow should be paused
        # because there is no task in active status.
        self.forward_task_statuses(conductor, "task2", [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the workflow, task3 should be staged, and complete task3.
        conductor.request_workflow_status(statuses.RESUMING)
        self.assert_next_task(conductor, "task3", {})
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

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

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Run task1 and task2.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Pause task1 and task2.
        self.forward_task_statuses(conductor, "task1", [statuses.PAUSED])
        self.forward_task_statuses(conductor, "task2", [statuses.PAUSED])
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume and complete task1 only. Once task1 completes, the workflow
        # should pause again because there is no active task.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume and complete task2. When task2 completes, the workflow
        # should stay running because task3 is now staged and ready.
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.forward_task_statuses(conductor, "task2", [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assert_next_task(conductor, "task3", {})

        # Complete task3.
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_pause_and_failed_with_task_transition_error(self):
        wf_def = """
        version: 1.0
        description: A basic sequential workflow.
        tasks:
          task1:
            action: core.noop
            next:
              - when: <% result().foobar %>
                do: task2
          task2:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Run task1.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

        # Complete task1 and assert the workflow execution fails
        # due to the expression error in the task transition.
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_pause_workflow_already_pausing(self):
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

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Run task1 and task2.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)

        # Complete task1 only. The workflow should still be pausing
        # because task2 is still running.
        self.forward_task_statuses(conductor, "task1", [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)
        conductor.request_workflow_status(statuses.PAUSED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

        # Complete task2. When task2 completes, the workflow should be paused
        # because there is no task in active status.
        self.forward_task_statuses(conductor, "task2", [statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the workflow, task3 should be staged, and complete task3.
        conductor.request_workflow_status(statuses.RESUMING)
        self.assert_next_task(conductor, "task3", {})
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
