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

import unittest

from orquesta import events
from orquesta import exceptions as exc
from orquesta import machines
from orquesta import statuses


class MockExecutionEvent(events.ActionExecutionEvent):
    def __init__(self, name, status):
        self.name = name
        self.status = status


class TaskStateMachineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TaskStateMachineTest, cls).setUpClass()
        statuses.ALL_STATUSES.append("mock")

    @classmethod
    def tearDownClass(cls):
        statuses.ALL_STATUSES.remove("mock")
        super(TaskStateMachineTest, cls).tearDownClass()

    def test_bad_event_name(self):
        task_flow_entry = {"id": "task1", "route": 0, "ctx": 0}
        mock_event = MockExecutionEvent("foobar", statuses.RUNNING)

        self.assertRaises(
            exc.InvalidEvent,
            machines.TaskStateMachine.process_event,
            None,
            task_flow_entry,
            mock_event,
        )

    def test_bad_event_status(self):
        self.assertRaises(exc.InvalidStatus, events.ExecutionEvent, "mock_event", "foobar")

    def test_bad_current_task_status(self):
        task_flow_entry = {"id": "task1", "route": 0, "ctx": 0, "status": "mock"}
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)

        self.assertRaises(
            exc.InvalidTaskStatusTransition,
            machines.TaskStateMachine.process_event,
            None,
            task_flow_entry,
            ac_ex_event,
        )

    def test_bad_current_task_status_to_event_mapping(self):
        task_flow_entry = {"id": "task1", "route": 0, "ctx": 0, "status": statuses.REQUESTED}
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
        machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry["status"], statuses.REQUESTED)

    def test_current_task_status_unset(self):
        task_flow_entry = {"id": "task1", "route": 0, "ctx": 0}
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry["status"], statuses.RUNNING)

    def test_current_task_status_none(self):
        task_flow_entry = {"id": "task1", "route": 0, "ctx": 0, "status": None}
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry["status"], statuses.RUNNING)

    def test_task_status_transition(self):
        task_flow_entry = {"id": "task1", "route": 0, "ctx": 0, "status": statuses.RUNNING}
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
        machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry["status"], statuses.SUCCEEDED)


class FailedStateTransitionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(FailedStateTransitionTest, cls).setUpClass()
        statuses.ALL_STATUSES.append("mock")

    @classmethod
    def tearDownClass(cls):
        statuses.ALL_STATUSES.remove("mock")
        super(FailedStateTransitionTest, cls).tearDownClass()

    def test_invalid_old_status(self):
        self.assertRaises(
            exc.InvalidStatus,
            machines.TaskStateMachine.is_transition_valid,
            "foobar",
            statuses.REQUESTED,
        )

    def test_invalid_new_status(self):
        self.assertRaises(
            exc.InvalidStatus,
            machines.TaskStateMachine.is_transition_valid,
            statuses.UNSET,
            "foobar",
        )

    def test_original_status_not_in_transition_map(self):
        self.assertFalse(machines.TaskStateMachine.is_transition_valid("mock", None))


class StateTransitionTest(unittest.TestCase):
    def test_null_statuses(self):
        is_transition_valid = machines.TaskStateMachine.is_transition_valid
        self.assertTrue(is_transition_valid(None, None))
        self.assertTrue(is_transition_valid(statuses.UNSET, None))
        self.assertTrue(is_transition_valid(None, statuses.UNSET))
        self.assertTrue(is_transition_valid(statuses.UNSET, statuses.UNSET))

    def test_transition(self):
        cases = [
            (x, y)
            for x in machines.TASK_STATE_MACHINE_DATA.keys()
            for y in machines.TASK_STATE_MACHINE_DATA[x].values()
        ]

        for x, y in cases:
            expected = x == y or y in machines.TASK_STATE_MACHINE_DATA[x].values()
            self.assertEqual(machines.TaskStateMachine.is_transition_valid(x, y), expected)
