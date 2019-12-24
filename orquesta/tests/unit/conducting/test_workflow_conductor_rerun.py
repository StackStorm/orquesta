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

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        fast_forward_statuses = [statuses.RUNNING, statuses.SUCCEEDED]
        self.forward_task_statuses(conductor, next_tasks[0]['id'], fast_forward_statuses)

        # Assert rerun cannot happen because workflow is still running.
        self.assertRaises(
            exc.WorkflowNotInRerunableStatusError,
            conductor.request_workflow_rerun
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
        """

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        fast_forward_statuses = [statuses.RUNNING, statuses.SUCCEEDED]
        self.forward_task_statuses(conductor, next_tasks[0]['id'], fast_forward_statuses)

        # Fail task2.
        next_tasks = conductor.get_next_tasks()
        fast_forward_statuses = [statuses.RUNNING, statuses.FAILED]
        self.forward_task_statuses(conductor, next_tasks[0]['id'], fast_forward_statuses)

        # Assert workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        # Assert rerun cannot happen because task1 is already succeeded.
        self.assertRaises(
            exc.InvalidTaskRerunRequest,
            conductor.request_workflow_rerun,
            tasks=[('task1', 0)]
        )

        # Assert rerun cannot happen because task3 does not exist.
        self.assertRaises(
            exc.InvalidTaskRerunRequest,
            conductor.request_workflow_rerun,
            tasks=[('task3', 0)]
        )

    def test_basic_rerun(self):
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

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Succeed task1.
        next_tasks = conductor.get_next_tasks()
        fast_forward_statuses = [statuses.RUNNING, statuses.SUCCEEDED]
        self.forward_task_statuses(conductor, next_tasks[0]['id'], fast_forward_statuses)

        # Fail task2.
        next_tasks = conductor.get_next_tasks()
        fast_forward_statuses = [statuses.RUNNING, statuses.FAILED]
        self.forward_task_statuses(conductor, next_tasks[0]['id'], fast_forward_statuses)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), {'foobar': 'foobar'})
        actual_task2_errors = [e for e in conductor.errors if e.get('task_id', None) == 'task2']
        self.assertGreater(len(actual_task2_errors), 0)

        # Request workflow rerun.
        conductor.request_workflow_rerun()

        # Assert workflow status is running and state is reset.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertIsNone(conductor.get_workflow_output())
        actual_task2_errors = [e for e in conductor.errors if e.get('task_id', None) == 'task2']
        self.assertEqual(len(actual_task2_errors), 0)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task2')

        # Assert sequence of tasks is correct.
        staged_task = conductor.workflow_state.get_staged_task('task2', 0)
        self.assertTrue(staged_task['ready'])
        self.assertDictEqual(staged_task['ctxs'], {'in': [0, 1]})
        self.assertDictEqual(staged_task['prev'], {'task1__t0': 0})

        task_state = conductor.workflow_state.get_task('task2', 0)
        self.assertDictEqual(task_state['ctxs'], {'in': [0, 1]})
        self.assertDictEqual(task_state['prev'], {'task1__t0': 0})
        self.assertDictEqual(task_state['next'], {})

        task_states = [
            t for t in list(enumerate(conductor.workflow_state.sequence))
            if t[1]['id'] == 'task2' and t[1]['route'] == 0
        ]

        self.assertEqual(len(task_states), 2)
        self.assertListEqual([t[0] for t in task_states], [1, 2])

        # Fail task2 again.
        next_tasks = conductor.get_next_tasks()
        fast_forward_statuses = [statuses.RUNNING, statuses.FAILED]
        self.forward_task_statuses(conductor, next_tasks[0]['id'], fast_forward_statuses)

        # Render workflow output and assert workflow status, error, and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), {'foobar': 'foobar'})
        actual_task2_errors = [e for e in conductor.errors if e.get('task_id', None) == 'task2']
        self.assertGreater(len(actual_task2_errors), 0)

        # Request workflow rerun from task.
        conductor.request_workflow_rerun(tasks=[('task2', 0)])

        # Assert workflow status is running and state is reset.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertIsNone(conductor.get_workflow_output())
        actual_task2_errors = [e for e in conductor.errors if e.get('task_id', None) == 'task2']
        self.assertEqual(len(actual_task2_errors), 0)
        next_tasks = conductor.get_next_tasks()
        self.assertEqual(len(next_tasks), 1)
        self.assertEqual(next_tasks[0]['id'], 'task2')

        # Assert sequence of tasks is correct.
        staged_task = conductor.workflow_state.get_staged_task('task2', 0)
        self.assertTrue(staged_task['ready'])
        self.assertDictEqual(staged_task['ctxs'], {'in': [0, 1]})
        self.assertDictEqual(staged_task['prev'], {'task1__t0': 0})

        task_state = conductor.workflow_state.get_task('task2', 0)
        self.assertDictEqual(task_state['ctxs'], {'in': [0, 1]})
        self.assertDictEqual(task_state['prev'], {'task1__t0': 0})
        self.assertDictEqual(task_state['next'], {})

        task_states = [
            t for t in list(enumerate(conductor.workflow_state.sequence))
            if t[1]['id'] == 'task2' and t[1]['route'] == 0
        ]

        self.assertEqual(len(task_states), 3)
        self.assertListEqual([t[0] for t in task_states], [1, 2, 3])

        # Succeed task2.
        next_tasks = conductor.get_next_tasks()
        fast_forward_statuses = [statuses.RUNNING, statuses.SUCCEEDED]
        self.forward_task_statuses(conductor, next_tasks[0]['id'], fast_forward_statuses)

        # Assert workflow is completed.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), {'foobar': 'fubar'})
