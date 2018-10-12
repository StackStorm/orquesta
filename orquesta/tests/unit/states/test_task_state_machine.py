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
from orquesta import states
from orquesta.states import machines


class MockExecutionEvent(events.ActionExecutionEvent):

    def __init__(self, name, state):
        self.name = name
        self.state = state


class TaskStateMachineTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TaskStateMachineTest, cls).setUpClass()
        states.ALL_STATES.append('mock')

    @classmethod
    def tearDownClass(cls):
        states.ALL_STATES.remove('mock')
        super(TaskStateMachineTest, cls).tearDownClass()

    def test_bad_event_name(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0}
        mock_event = MockExecutionEvent('foobar', states.RUNNING)

        self.assertRaises(
            exc.InvalidEvent,
            machines.TaskStateMachine.process_event,
            None,
            task_flow_entry,
            mock_event
        )

    def test_bad_event_state(self):
        self.assertRaises(
            exc.InvalidState,
            events.ExecutionEvent,
            'mock_event',
            'foobar'
        )

    def test_bad_current_task_state(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0, 'state': 'mock'}
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED)

        self.assertRaises(
            exc.InvalidTaskStateTransition,
            machines.TaskStateMachine.process_event,
            None,
            task_flow_entry,
            ac_ex_event
        )

    def test_bad_current_task_state_to_event_mapping(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0, 'state': states.REQUESTED}
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED)
        machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry['state'], states.REQUESTED)

    def test_current_task_state_unset(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0}
        ac_ex_event = events.ActionExecutionEvent(states.RUNNING)
        machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry['state'], states.RUNNING)

    def test_current_task_state_none(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0, 'state': None}
        ac_ex_event = events.ActionExecutionEvent(states.RUNNING)
        machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry['state'], states.RUNNING)

    def test_task_state_transition(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0, 'state': states.RUNNING}
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED)
        machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry['state'], states.SUCCEEDED)


class FailedStateTransitionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(FailedStateTransitionTest, cls).setUpClass()
        states.ALL_STATES.append('mock')

    @classmethod
    def tearDownClass(cls):
        states.ALL_STATES.remove('mock')
        super(FailedStateTransitionTest, cls).tearDownClass()

    def test_invalid_old_state(self):
        self.assertRaises(
            exc.InvalidState,
            machines.TaskStateMachine.is_transition_valid,
            'foobar',
            states.REQUESTED
        )

    def test_invalid_new_state(self):
        self.assertRaises(
            exc.InvalidState,
            machines.TaskStateMachine.is_transition_valid,
            states.UNSET,
            'foobar'
        )

    def test_original_state_not_in_transition_map(self):
        self.assertFalse(machines.TaskStateMachine.is_transition_valid('mock', None))


class StateTransitionTest(unittest.TestCase):

    def test_null_states(self):
        is_transition_valid = machines.TaskStateMachine.is_transition_valid
        self.assertTrue(is_transition_valid(None, None))
        self.assertTrue(is_transition_valid(states.UNSET, None))
        self.assertTrue(is_transition_valid(None, states.UNSET))
        self.assertTrue(is_transition_valid(states.UNSET, states.UNSET))

    def test_transition(self):
        cases = [
            (x, y)
            for x in machines.TASK_STATE_MACHINE_DATA.keys()
            for y in machines.TASK_STATE_MACHINE_DATA[x].values()
        ]

        for x, y in cases:
            expected = (x == y or y in machines.TASK_STATE_MACHINE_DATA[x].values())
            self.assertEqual(machines.TaskStateMachine.is_transition_valid(x, y), expected)
