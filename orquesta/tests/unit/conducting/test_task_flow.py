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
from orquesta import exceptions as exc
from orquesta import graphing
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorTaskStateTest(test_base.WorkflowConductorTest):
    def _add_tasks(self, wf_graph):
        for i in range(1, 6):
            task_name = "task" + str(i)
            wf_graph.add_task(task_name, name=task_name)

    def _add_transitions(self, wf_graph):
        wf_graph.add_transition("task1", "task2")
        wf_graph.add_transition("task1", "task5")
        wf_graph.add_transition("task2", "task3")
        wf_graph.add_transition("task3", "task4")
        wf_graph.add_transition("task4", "task2")

    def _prep_graph(self):
        wf_graph = graphing.WorkflowGraph()

        self._add_tasks(wf_graph)
        self._add_transitions(wf_graph)

        return wf_graph

    def _prep_conductor(self, context=None, inputs=None, status=None):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2, task5
          task2:
            action: core.noop
            next:
              - do: task3
          task3:
            action: core.noop
            next:
              - do: task4
          task4:
            action: core.noop
            next:
              - do: task2
          task5:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)

        kwargs = {
            "context": context if context is not None else None,
            "inputs": inputs if inputs is not None else None,
        }

        conductor = conducting.WorkflowConductor(spec, **kwargs)

        if status:
            conductor.request_workflow_status(status)

        return conductor

    def test_add_task_state(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)

        task_route = 0
        task_name = "task1"
        conductor.add_task_state(task_name, task_route)

        expected_task_state_idx = 0
        actual_task_state_idx = conductor._get_task_state_idx(task_name, task_route)
        self.assertEqual(actual_task_state_idx, expected_task_state_idx)

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "next": {},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

    def test_add_task_state_no_context(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)

        task_route = 0
        task_name = "task1"
        conductor.add_task_state(task_name, task_route)

        expected_task_state_idx = 0
        actual_task_state_idx = conductor._get_task_state_idx(task_name, task_route)
        self.assertEqual(actual_task_state_idx, expected_task_state_idx)

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "next": {},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

    def test_update_task_state(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)

        task_route = 0
        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        expected_task_state_idx = 0
        actual_task_state_idx = conductor._get_task_state_idx(task_name, task_route)
        self.assertEqual(actual_task_state_idx, expected_task_state_idx)

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "running",
            "next": {},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED], result="foobar")

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "succeeded",
            "next": {"task2__t0": True, "task5__t0": True},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

    def test_update_task_state_with_failed_event(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)

        task_route = 0
        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        expected_task_state_idx = 0
        actual_task_state_idx = conductor._get_task_state_idx(task_name, task_route)
        self.assertEqual(actual_task_state_idx, expected_task_state_idx)

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "running",
            "next": {},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

        self.forward_task_statuses(
            conductor, task_name, [statuses.FAILED], result={"stdout": "boom!"}
        )

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "failed",
            "next": {"task2__t0": False, "task5__t0": False},
            "prev": {},
            "term": True,
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

        expected_errors = [
            {
                "type": "error",
                "message": "Execution failed. See result for details.",
                "task_id": "task1",
                "result": {"stdout": "boom!"},
            }
        ]

        self.assertListEqual(conductor.errors, expected_errors)

    def test_update_task_state_for_not_ready_task(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)

        self.assertRaises(
            exc.InvalidTaskStateEntry,
            conductor.update_task_state,
            "task2",
            0,
            events.ActionExecutionEvent(statuses.RUNNING),
        )

    def test_update_task_state_for_nonexistent_task(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)

        self.assertRaises(
            exc.InvalidTask,
            conductor.update_task_state,
            "task999",
            0,
            events.ActionExecutionEvent(statuses.RUNNING),
        )

    def test_update_invalid_status_to_task_state_item(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)

        task_route = 0
        task_name = "task1"

        self.assertRaises(exc.InvalidStatus, events.ActionExecutionEvent, "foobar")

        self.assertRaises(TypeError, conductor.update_task_state, task_name, task_route, "foobar")

        self.forward_task_statuses(conductor, task_name, [statuses.REQUESTED])

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "requested",
            "next": {},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

        # When transition is not valid, the task status is not changed. For the test case below,
        # the status change from requested to succeeded is not a valid transition.
        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED])

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "requested",
            "next": {},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

    def test_add_sequence_to_task_state(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)

        # Update progress of task1 to task flow.
        task_route = 0
        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        expected_task_state_idx = 0
        actual_task_state_idx = conductor._get_task_state_idx(task_name, task_route)
        self.assertEqual(actual_task_state_idx, expected_task_state_idx)

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "running",
            "next": {},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED], result="foobar")

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "succeeded",
            "next": {"task2__t0": True, "task5__t0": True},
            "prev": {},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

        expected_sequence = [expected_task_state_entry]
        self.assertListEqual(conductor.workflow_state.sequence, expected_sequence)

        # Update progress of task2 to task flow.
        task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        expected_task_state_idx = 1
        actual_task_state_idx = conductor._get_task_state_idx(task_name, task_route)
        self.assertEqual(actual_task_state_idx, expected_task_state_idx)

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "running",
            "next": {},
            "prev": {"task1__t0": 0},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED], result="foobar")

        expected_task_state_entry = {
            "id": task_name,
            "route": task_route,
            "ctxs": {"in": [0]},
            "status": "succeeded",
            "next": {"task3__t0": True},
            "prev": {"task1__t0": 0},
        }

        actual_task_state_entry = conductor.get_task_state_entry(task_name, task_route)
        self.assertDictEqual(actual_task_state_entry, expected_task_state_entry)

        expected_sequence.append(expected_task_state_entry)
        self.assertListEqual(conductor.workflow_state.sequence, expected_sequence)

    def test_add_cycle_to_task_state(self):
        conductor = self._prep_conductor(status=statuses.RUNNING)
        task_route = 0

        # Check that there's a cycle in the graph.
        self.assertFalse(conductor.graph.in_cycle("task1"))
        self.assertTrue(conductor.graph.in_cycle("task2"))
        self.assertTrue(conductor.graph.in_cycle("task3"))
        self.assertTrue(conductor.graph.in_cycle("task4"))

        # Add a cycle to the task flow.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, "task3", [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, "task4", [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING])

        # Check the reference pointer. Task2 points to the latest instance.
        self.assertEqual(conductor._get_task_state_idx("task1", task_route), 0)
        self.assertEqual(conductor._get_task_state_idx("task2", task_route), 4)
        self.assertEqual(conductor._get_task_state_idx("task3", task_route), 2)
        self.assertEqual(conductor._get_task_state_idx("task4", task_route), 3)

        # Check sequence.
        expected_sequence = [
            {
                "id": "task1",
                "route": task_route,
                "ctxs": {"in": [0]},
                "status": "succeeded",
                "next": {"task2__t0": True, "task5__t0": True},
                "prev": {},
            },
            {
                "id": "task2",
                "route": task_route,
                "ctxs": {"in": [0]},
                "status": "succeeded",
                "next": {"task3__t0": True},
                "prev": {"task1__t0": 0},
            },
            {
                "id": "task3",
                "route": task_route,
                "ctxs": {"in": [0]},
                "status": "succeeded",
                "next": {"task4__t0": True},
                "prev": {"task2__t0": 1},
            },
            {
                "id": "task4",
                "route": task_route,
                "ctxs": {"in": [0]},
                "status": "succeeded",
                "next": {"task2__t0": True},
                "prev": {"task3__t0": 2},
            },
            {
                "id": "task2",
                "route": task_route,
                "ctxs": {"in": [0]},
                "status": "running",
                "next": {},
                "prev": {"task4__t0": 3},
            },
        ]

        self.assertListEqual(conductor.workflow_state.sequence, expected_sequence)
