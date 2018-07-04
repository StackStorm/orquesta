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

from orchestra import events
from orchestra import exceptions as exc
from orchestra import states
from orchestra.states import machines


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
        ac_ex_event = events.ExecutionEvent('foobar', states.RUNNING)

        self.assertRaises(
            exc.InvalidEvent,
            machines.TaskStateMachine.process_event,
            task_flow_entry,
            ac_ex_event
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
            task_flow_entry,
            ac_ex_event
        )

    def test_bad_current_task_state_to_event_mapping(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0, 'state': states.REQUESTED}
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED)

        self.assertRaises(
            exc.InvalidTaskStateTransition,
            machines.TaskStateMachine.process_event,
            task_flow_entry,
            ac_ex_event
        )

    def test_current_task_state_unset(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0}
        ac_ex_event = events.ActionExecutionEvent(states.RUNNING)
        task_flow_entry = machines.TaskStateMachine.process_event(task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry['state'], states.RUNNING)

    def test_current_task_state_none(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0, 'state': None}
        ac_ex_event = events.ActionExecutionEvent(states.RUNNING)
        task_flow_entry = machines.TaskStateMachine.process_event(task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry['state'], states.RUNNING)

    def test_task_state_transition(self):
        task_flow_entry = {'id': 'task1', 'ctx': 0, 'state': states.RUNNING}
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED)
        task_flow_entry = machines.TaskStateMachine.process_event(task_flow_entry, ac_ex_event)
        self.assertEqual(task_flow_entry['state'], states.SUCCEEDED)
