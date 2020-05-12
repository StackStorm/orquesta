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
from orquesta import events
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorEvalTaskRetryTest(test_base.WorkflowConductorTest):
    def test_no_task_retry(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        # Setup workflow conductor.
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Forward task1 from running to failed.
        task_id = "task1"
        route = 0
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(task_id, route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.FAILED)
        conductor.update_task_state(task_id, route, ac_ex_event)

        # Get task state for task1.
        task_state_entry = conductor.get_task_state_entry(task_id, route)

        # Set context for task1.
        current_task_ctx = conductor.make_task_context(task_state_entry, task_result=None)

        # Assert retry is false for task1.
        self.assertFalse(conductor._evaluate_task_retry(task_state_entry, current_task_ctx))

    def test_retries_exhausted(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            retry:
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        # Setup workflow conductor.
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Forward task1 from running to failed.
        task_id = "task1"
        route = 0
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(task_id, route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.FAILED)
        conductor.update_task_state(task_id, route, ac_ex_event)

        # Get task state for task1.
        task_state_entry = conductor.get_task_state_entry(task_id, route)
        task_state_entry["retry"]["tally"] = 3

        # Set context for task1.
        current_task_ctx = conductor.make_task_context(task_state_entry, task_result=None)

        # Assert retry is false for task1.
        self.assertFalse(conductor._evaluate_task_retry(task_state_entry, current_task_ctx))

    def test_retry_default_condition_satisfied(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            retry:
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        # Setup workflow conductor.
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Forward task1 from running to failed.
        task_id = "task1"
        route = 0
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(task_id, route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.FAILED)
        conductor.update_task_state(task_id, route, ac_ex_event)

        # Get task state for task1.
        task_state_entry = conductor.get_task_state_entry(task_id, route)

        # Mock the task status.
        task_state_entry["status"] = statuses.FAILED

        # Set context for task1.
        current_task_ctx = conductor.make_task_context(task_state_entry, task_result=None)

        # Assert retry is true for task1.
        self.assertTrue(conductor._evaluate_task_retry(task_state_entry, current_task_ctx))

    def test_retry_default_condition_not_satisfied(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            retry:
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        # Setup workflow conductor.
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Forward task1 from running to succeeded.
        task_id = "task1"
        route = 0
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(task_id, route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
        conductor.update_task_state(task_id, route, ac_ex_event)

        # Get task state for task1.
        task_state_entry = conductor.get_task_state_entry(task_id, route)

        # Set context for task1.
        current_task_ctx = conductor.make_task_context(task_state_entry, task_result=None)

        # Assert retry is false for task1.
        self.assertFalse(conductor._evaluate_task_retry(task_state_entry, current_task_ctx))

    def test_retry_given_condition_satisfied(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            retry:
              when: <% result().status_code = 400 %>
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        # Setup workflow conductor.
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Forward task1 from running to succeeded.
        task_id = "task1"
        route = 0
        task_result = {"status_code": 400}
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(task_id, route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED, result=task_result)
        conductor.update_task_state(task_id, route, ac_ex_event)

        # Get task state for task1.
        task_state_entry = conductor.get_task_state_entry(task_id, route)

        # Set context for task1.
        current_task_ctx = conductor.make_task_context(task_state_entry, task_result=task_result)

        # Assert retry is true for task1.
        self.assertTrue(conductor._evaluate_task_retry(task_state_entry, current_task_ctx))

    def test_retry_given_condition_not_satisfied(self):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            retry:
              when: <% result().status_code = 400 %>
              count: 3
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        # Setup workflow conductor.
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Forward task1 from running to succeeded.
        task_id = "task1"
        route = 0
        task_result = {"status_code": 200}
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        conductor.update_task_state(task_id, route, ac_ex_event)
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED, result=task_result)
        conductor.update_task_state(task_id, route, ac_ex_event)

        # Get task state for task1.
        task_state_entry = conductor.get_task_state_entry(task_id, route)

        # Set context for task1.
        current_task_ctx = conductor.make_task_context(task_state_entry, task_result=task_result)

        # Assert retry is false for task1.
        self.assertFalse(conductor._evaluate_task_retry(task_state_entry, current_task_ctx))
