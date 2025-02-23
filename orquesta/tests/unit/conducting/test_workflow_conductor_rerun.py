# Copyright 2021 The StackStorm Authors.
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
from orquesta import exceptions as exc
from orquesta import requests
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorRerunTest(test_base.WorkflowConductorTest):
    def test_workflow_not_in_rerunable_status(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert rerun cannot happen because workflow is still running.
        self.assertRaises(
            exc.WorkflowIsActiveAndNotRerunableError, conductor.request_workflow_rerun
        )

    def test_task_not_in_rerunable_status(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
          task3:
            action: core.noop
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Fail task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Assert workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # Assert rerun cannot happen because task3 has not happened yet.
        self.assertRaises(
            exc.InvalidTaskRerunRequest,
            conductor.request_workflow_rerun,
            task_requests=[requests.TaskRerunRequest.new("task3", route=0)],
        )

        # Assert rerun cannot happen because task4 does not exists.
        self.assertRaises(
            exc.InvalidTaskRerunRequest,
            conductor.request_workflow_rerun,
            task_requests=[requests.TaskRerunRequest.new("task4", route=0)],
        )

    def test_rerun_from_failed_task(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% succeeded() %>
                publish: foobar="foobar"
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: foobar="fubar"
                do: task3
          task3:
            action: core.noop

        output:
          - foobar: <% ctx().foobar %>
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Fail task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "foobar"})
        actual_task2_errors = [e for e in conductor.errors if e.get("task_id", None) == "task2"]
        self.assertGreater(len(actual_task2_errors), 0)

        # Request workflow rerun.
        conductor.request_workflow_rerun()

        # Assert workflow status is resuming and state is reset.
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)
        self.assertIsNone(conductor.get_workflow_output())
        actual_task2_errors = [e for e in conductor.errors if e.get("task_id", None) == "task2"]
        self.assertEqual(len(actual_task2_errors), 0)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task2")

        # Assert sequence of tasks is correct.
        staged_task = conductor.workflow_state.get_staged_task("task2", 0)
        self.assertTrue(staged_task["ready"])
        self.assertDictEqual(staged_task["ctxs"], {"in": [0, 1]})
        self.assertDictEqual(staged_task["prev"], {"task1__t0": 0})

        task_state = conductor.workflow_state.get_task("task2", 0)
        self.assertDictEqual(task_state["ctxs"], {"in": [0, 1]})
        self.assertDictEqual(task_state["prev"], {"task1__t0": 0})
        self.assertDictEqual(task_state["next"], {})

        task_states = [
            t
            for t in list(enumerate(conductor.workflow_state.sequence))
            if t[1]["id"] == "task2" and t[1]["route"] == 0
        ]

        self.assertEqual(len(task_states), 2)
        self.assertListEqual([t[0] for t in task_states], [1, 2])

        # Fail task2 again.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "foobar"})
        actual_task2_errors = [e for e in conductor.errors if e.get("task_id", None) == "task2"]
        self.assertGreater(len(actual_task2_errors), 0)

        # Assert there is a rerun record.
        expected_rerun_records = [[1]]
        self.assertListEqual(conductor.workflow_state.reruns, expected_rerun_records)

        # Request workflow rerun from task.
        task_rerun_req = requests.TaskRerunRequest.new("task2", route=0)
        conductor.request_workflow_rerun(task_requests=[task_rerun_req])

        # Assert workflow status is resuming and state is reset.
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)
        self.assertIsNone(conductor.get_workflow_output())
        actual_task2_errors = [e for e in conductor.errors if e.get("task_id", None) == "task2"]
        self.assertEqual(len(actual_task2_errors), 0)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task2")

        # Assert sequence of tasks is correct.
        staged_task = conductor.workflow_state.get_staged_task("task2", 0)
        self.assertTrue(staged_task["ready"])
        self.assertDictEqual(staged_task["ctxs"], {"in": [0, 1]})
        self.assertDictEqual(staged_task["prev"], {"task1__t0": 0})

        task_state = conductor.workflow_state.get_task("task2", 0)
        self.assertDictEqual(task_state["ctxs"], {"in": [0, 1]})
        self.assertDictEqual(task_state["prev"], {"task1__t0": 0})
        self.assertDictEqual(task_state["next"], {})

        task_states = [
            t
            for t in list(enumerate(conductor.workflow_state.sequence))
            if t[1]["id"] == "task2" and t[1]["route"] == 0
        ]

        self.assertEqual(len(task_states), 3)
        self.assertListEqual([t[0] for t in task_states], [1, 2, 3])

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert workflow is still running.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Succeed task3.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task3")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert workflow is completed.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "fubar"})

        # Assert there are two separate rerun records.
        expected_rerun_records = [[1], [2]]
        self.assertListEqual(conductor.workflow_state.reruns, expected_rerun_records)

    def test_rerun_from_succeeded_task(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% succeeded() %>
                publish: foobar="foobar"
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: foobar="fubar"
                do: task3
          task3:
            action: core.noop

        output:
          - foobar: <% ctx().foobar %>
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Fail task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "foobar"})
        self.assertIn("task2", [e["task_id"] for e in conductor.errors])

        # Request workflow rerun from the succeeded task1 previous to the failed task2.
        task_rerun_req = requests.TaskRerunRequest.new("task1", route=0)
        conductor.request_workflow_rerun(task_requests=[task_rerun_req])

        # Assert workflow status is resuming and state is reset.
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)
        self.assertIsNone(conductor.get_workflow_output())
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Assert sequence of tasks is correct.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task("task1", 0))
        self.assertIsNone(conductor.workflow_state.get_staged_task("task2", 0))

        task_states = [
            t
            for t in list(enumerate(conductor.workflow_state.sequence))
            if t[1]["id"] == "task1" and t[1]["route"] == 0
        ]

        self.assertEqual(len(task_states), 2)
        self.assertListEqual([t[0] for t in task_states], [0, 2])

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert workflow is still running.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Succeed task3.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task3")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert workflow is completed.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "fubar"})

    def test_rerun_succeeded_workflow(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% succeeded() %>
                publish: foobar="foobar"
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: foobar="fubar"

        output:
          - foobar: <% ctx().foobar %>
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert workflow is completed.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "fubar"})

        # Request workflow rerun from the succeeded task1.
        task_rerun_req = requests.TaskRerunRequest.new("task1", route=0)
        conductor.request_workflow_rerun(task_requests=[task_rerun_req])

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Fail task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "foobar"})
        task2 = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(task2["status"], statuses.FAILED)
        self.assertIn("task2", [e["task_id"] for e in conductor.errors])

        # Request workflow rerun.
        conductor.request_workflow_rerun()

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert workflow is completed.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "fubar"})

    def test_rerun_default_with_remediated_task(self):
        wf_def = """
        version: 1.0
        vars:
          - foobar: null
          - fubar: null
        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% failed() %>
                publish: foobar="fubar"
                do: task3
              - when: <% succeeded() %>
                publish: foobar="foobar"
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: fubar="fubar"
          task3:
            action: core.noop
        output:
          - foobar: <% ctx().foobar %>
          - fubar: <% ctx().fubar %>
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Fail task1 to trigger the remediation task.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Succeed the remediation task3.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[1]["id"], "task3")
        self.forward_task_statuses(conductor, next_tasks[1]["id"], fast_forward_success)

        # Fail task2 to stop the workflow.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        task1 = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(task1["status"], statuses.FAILED)
        self.assertIn("task1", [e["task_id"] for e in conductor.errors])
        task2 = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(task2["status"], statuses.FAILED)
        self.assertIn("task2", [e["task_id"] for e in conductor.errors])

        # Request workflow rerun. By default, since task1 is remediated, only task2 will be rerun.
        conductor.request_workflow_rerun()

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Render workflow output and assert workflow status, error, and output.
        self.assertEqual(len(conductor.get_next_tasks()), 0)
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        task1 = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(task1["status"], statuses.FAILED)
        self.assertIn("task1", [e["task_id"] for e in conductor.errors])
        task2 = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(task2["status"], statuses.SUCCEEDED)
        self.assertNotIn("task2", [e["task_id"] for e in conductor.errors])
        expected_workflow_output = {"foobar": "fubar", "fubar": "fubar"}
        self.assertDictEqual(conductor.get_workflow_output(), expected_workflow_output)

    def test_rerun_from_remediated_task(self):
        wf_def = """
        version: 1.0
        vars:
          - foobar: null
          - fubar: null
        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% failed() %>
                publish: foobar="fubar"
                do: task3
              - when: <% succeeded() %>
                publish: foobar="foobar"
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: fubar="fubar"
          task3:
            action: core.noop
        output:
          - foobar: <% ctx().foobar %>
          - fubar: <% ctx().fubar %>
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Fail task1 to trigger the remediation task.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Succeed the remediation task3.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[1]["id"], "task3")
        self.forward_task_statuses(conductor, next_tasks[1]["id"], fast_forward_success)

        # Fail task2 to stop the workflow.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        task1 = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(task1["status"], statuses.FAILED)
        self.assertIn("task1", [e["task_id"] for e in conductor.errors])
        task2 = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(task2["status"], statuses.FAILED)
        self.assertIn("task2", [e["task_id"] for e in conductor.errors])

        # Request workflow rerun and explicitly ask the remediated task1 to rerun.
        tk1_rerun_req = requests.TaskRerunRequest.new("task1", route=0)
        tk2_rerun_req = requests.TaskRerunRequest.new("task2", route=0)
        conductor.request_workflow_rerun(task_requests=[tk1_rerun_req, tk2_rerun_req])

        # Succeed task1 and task2.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)
        self.assertEqual(next_tasks[1]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[1]["id"], fast_forward_success)

        # Render workflow output and assert workflow status, error, and output.
        self.assertEqual(len(conductor.get_next_tasks()), 0)
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        task1 = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(task1["status"], statuses.SUCCEEDED)
        self.assertNotIn("task1", [e["task_id"] for e in conductor.errors])
        task2 = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(task2["status"], statuses.SUCCEEDED)
        self.assertNotIn("task2", [e["task_id"] for e in conductor.errors])
        expected_workflow_output = {"foobar": "foobar", "fubar": "fubar"}
        self.assertDictEqual(conductor.get_workflow_output(), expected_workflow_output)

    def test_rerun_failed_task_with_items_and_reset(self):
        wf_def = """
        version: 1.0

        vars:
          - items: []
          - xs:
              - fee
              - fi
              - fo
              - fum

        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% succeeded() %>
                publish:
                  - items: <% result() %>
                do: task2
          task2:
            action: core.noop

        output:
          - items: <% ctx(items) %>
        """

        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Fail some item(s) to fail the workflow.
        task_items = ["fee", "fi", "fo", "fum"]
        task_items_status = [statuses.SUCCEEDED] * 3 + [statuses.FAILED]

        for idx, task_item in enumerate(task_items):
            fast_forward_statuses = [statuses.RUNNING, task_items_status[idx]]
            self.forward_task_item_statuses(
                conductor,
                next_tasks[0]["id"],
                idx,
                fast_forward_statuses,
                result=task_item,
                accumulated_result=task_items[0 : idx + 1],
            )

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        task1 = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(task1["status"], statuses.FAILED)
        self.assertIn("task1", [e["task_id"] for e in conductor.errors if e.get("task_id")])
        self.assertDictEqual(conductor.get_workflow_output(), {"items": []})

        # Request workflow rerun.
        task_rerun_req = requests.TaskRerunRequest.new("task1", route=0, reset_items=True)
        conductor.request_workflow_rerun(task_requests=[task_rerun_req])

        # Assert items are preserved in get next tasks.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(len(next_tasks[0]["actions"]), 4)

        # Succeed task1.
        task_items = ["fee", "fi", "fo", "fum"]
        task_items_status = [statuses.SUCCEEDED] * 4

        for idx, task_item in enumerate(task_items):
            fast_forward_statuses = [statuses.RUNNING, task_items_status[idx]]
            self.forward_task_item_statuses(
                conductor,
                next_tasks[0]["id"],
                idx,
                fast_forward_statuses,
                result=task_item,
                accumulated_result=task_items[0 : idx + 1],
            )

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Render workflow output and assert workflow status, error, and output.
        self.assertEqual(len(conductor.get_next_tasks()), 0)
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        task1 = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(task1["status"], statuses.SUCCEEDED)
        self.assertNotIn("task1", [e["task_id"] for e in conductor.errors])
        task2 = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(task2["status"], statuses.SUCCEEDED)
        self.assertNotIn("task2", [e["task_id"] for e in conductor.errors])
        expected_workflow_output = {"items": ["fee", "fi", "fo", "fum"]}
        self.assertDictEqual(conductor.get_workflow_output(), expected_workflow_output)

    def test_rerun_failed_task_with_items_and_no_reset(self):
        wf_def = """
        version: 1.0

        vars:
          - items: []
          - xs:
              - fee
              - fi
              - fo
              - fum

        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% succeeded() %>
                publish:
                  - items: <% result() %>
                do: task2
          task2:
            action: core.noop

        output:
          - items: <% ctx(items) %>
        """

        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Fail some item(s) to fail the workflow.
        task_items = ["fee", "fi", "fo", "fum"]
        task_items_status = [statuses.SUCCEEDED] * 3 + [statuses.FAILED]

        for idx, task_item in enumerate(task_items):
            fast_forward_statuses = [statuses.RUNNING, task_items_status[idx]]
            self.forward_task_item_statuses(
                conductor,
                next_tasks[0]["id"],
                idx,
                fast_forward_statuses,
                result=task_item,
                accumulated_result=task_items[0 : idx + 1],
            )

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        task1 = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(task1["status"], statuses.FAILED)
        self.assertIn("task1", [e["task_id"] for e in conductor.errors if e.get("task_id")])
        self.assertDictEqual(conductor.get_workflow_output(), {"items": []})

        # Request workflow rerun.
        conductor.request_workflow_rerun()

        # Assert only 1 item is rendered in get next tasks.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.assertEqual(len(next_tasks[0]["actions"]), 1)

        # Succeed last item for task1.
        task_items = ["fee", "fi", "fo", "fum"]
        item_id = next_tasks[0]["actions"][0]["item_id"]

        self.forward_task_item_statuses(
            conductor,
            next_tasks[0]["id"],
            item_id,
            fast_forward_success,
            result=task_items[item_id],
            accumulated_result=task_items,
        )

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Render workflow output and assert workflow status, error, and output.
        self.assertEqual(len(conductor.get_next_tasks()), 0)
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        task1 = conductor.get_task_state_entry("task1", 0)
        self.assertEqual(task1["status"], statuses.SUCCEEDED)
        self.assertNotIn("task1", [e["task_id"] for e in conductor.errors])
        task2 = conductor.get_task_state_entry("task2", 0)
        self.assertEqual(task2["status"], statuses.SUCCEEDED)
        self.assertNotIn("task2", [e["task_id"] for e in conductor.errors])
        expected_workflow_output = {"items": ["fee", "fi", "fo", "fum"]}
        self.assertDictEqual(conductor.get_workflow_output(), expected_workflow_output)

    def test_rerun_from_failed_task_with_implicit_continue(self):
        wf_def = """
        version: 1.0

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - publish: foobar="foobar"

        output:
          - foobar: <% ctx().foobar %>
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Fail task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # Request workflow rerun.
        conductor.request_workflow_rerun()

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Render workflow output and assert workflow status, error, and output.
        self.assertEqual(len(conductor.get_next_tasks()), 0)
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        expected_workflow_output = {"foobar": "foobar"}
        self.assertDictEqual(conductor.get_workflow_output(), expected_workflow_output)

    def test_rerun_from_multiple_sequential_tasks(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% succeeded() %>
                publish: foobar="foobar"
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: foobar="fubar"

        output:
          - foobar: <% ctx().foobar %>
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Fail task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "foobar"})
        self.assertIn("task2", [e["task_id"] for e in conductor.errors])

        # Request workflow rerun from both the succeeded task1 and the failed task2.
        tk1_rerun_req = requests.TaskRerunRequest.new("task1", route=0)
        tk2_rerun_req = requests.TaskRerunRequest.new("task2", route=0)
        conductor.request_workflow_rerun(task_requests=[tk1_rerun_req, tk2_rerun_req])

        # Assert workflow status is resuming and state is reset. We expect only task1
        # is setup to rerun since task2 is dependent on task1.
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)
        self.assertIsNone(conductor.get_workflow_output())
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Assert sequence of tasks is correct.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task("task1", 0))
        self.assertIsNone(conductor.workflow_state.get_staged_task("task2", 0))

        task_states = [
            t
            for t in list(enumerate(conductor.workflow_state.sequence))
            if t[1]["id"] == "task1" and t[1]["route"] == 0
        ]

        self.assertEqual(len(task_states), 2)
        self.assertListEqual([t[0] for t in task_states], [0, 2])

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert workflow is completed.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "fubar"})

    def test_rerun_with_duplicate_task_requests(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.echo message="$RANDOM"
            next:
              - when: <% succeeded() %>
                publish: foobar="foobar"
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: foobar="fubar"

        output:
          - foobar: <% ctx().foobar %>
        """

        fast_forward_failure = [statuses.RUNNING, statuses.FAILED]
        fast_forward_success = [statuses.RUNNING, statuses.SUCCEEDED]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Fail task2.
        next_tasks = conductor.get_next_tasks()
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_failure)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "foobar"})
        self.assertIn("task2", [e["task_id"] for e in conductor.errors])

        # Request workflow rerun from the succeeded task1. Request task1 twice.
        tk1_rerun_req = requests.TaskRerunRequest.new("task1", route=0)
        tk2_rerun_req = requests.TaskRerunRequest.new("task1", route=0)
        conductor.request_workflow_rerun(task_requests=[tk1_rerun_req, tk2_rerun_req])

        # Assert workflow status is resuming and state is reset. We expect only
        # a single instance of task1 is setup.
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)
        self.assertIsNone(conductor.get_workflow_output())
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]["id"], "task1")

        # Assert sequence of tasks is correct.
        self.assertIsNotNone(conductor.workflow_state.get_staged_task("task1", 0))
        self.assertIsNone(conductor.workflow_state.get_staged_task("task2", 0))

        task_states = [
            t
            for t in list(enumerate(conductor.workflow_state.sequence))
            if t[1]["id"] == "task1" and t[1]["route"] == 0
        ]

        self.assertEqual(len(task_states), 2)
        self.assertListEqual([t[0] for t in task_states], [0, 2])

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task1")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(next_tasks[0]["id"], "task2")
        self.forward_task_statuses(conductor, next_tasks[0]["id"], fast_forward_success)

        # Assert workflow is completed.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), {"foobar": "fubar"})
