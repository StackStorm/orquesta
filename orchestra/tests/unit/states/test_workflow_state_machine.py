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

from orchestra import exceptions as exc
from orchestra import states
from orchestra.states import machines


class WorkflowStateMachineTest(unittest.TestCase):
    pass


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
            machines.WorkflowStateMachine.is_transition_valid,
            'foobar',
            states.REQUESTED
        )

    def test_invalid_new_state(self):
        self.assertRaises(
            exc.InvalidState,
            machines.WorkflowStateMachine.is_transition_valid,
            states.UNSET,
            'foobar'
        )

    def test_original_state_not_in_transition_map(self):
        self.assertFalse(machines.WorkflowStateMachine.is_transition_valid('mock', None))


class StateTransitionTest(unittest.TestCase):

    def test_null_states(self):
        is_transition_valid = machines.WorkflowStateMachine.is_transition_valid
        self.assertTrue(is_transition_valid(None, None))
        self.assertTrue(is_transition_valid(states.UNSET, None))
        self.assertTrue(is_transition_valid(None, states.UNSET))
        self.assertTrue(is_transition_valid(states.UNSET, states.UNSET))

    def test_transition(self):
        cases = [
            (x, y)
            for x in machines.WORKFLOW_STATE_MACHINE_DATA.keys()
            for y in machines.WORKFLOW_STATE_MACHINE_DATA[x].values()
        ]

        for x, y in cases:
            expected = (x == y or y in machines.WORKFLOW_STATE_MACHINE_DATA[x].values())
            self.assertEqual(machines.WorkflowStateMachine.is_transition_valid(x, y), expected)
