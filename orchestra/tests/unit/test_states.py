# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from orchestra import exceptions as exc
from orchestra import states


class FailedStateTransitionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(FailedStateTransitionTest, cls).setUpClass()
        states.ALL_STATES.append('mock')

    @classmethod
    def tearDownClass(cls):
        states.ALL_STATES.remove('mock')
        super(FailedStateTransitionTest, cls).tearDownClass()

    def test_invalid_state(self):
        self.assertRaises(exc.InvalidState, states.is_transition_valid, 'foobar', states.REQUESTED)
        self.assertRaises(exc.InvalidState, states.is_transition_valid, states.UNSET, 'foobar')

    def test_invalid_state_transition(self):
        self.assertRaises(exc.InvalidStateTransition, states.is_transition_valid, 'mock', None)


class StateTransitionTest(unittest.TestCase):

    def test_null_states(self):
        self.assertTrue(states.is_transition_valid(None, None))
        self.assertTrue(states.is_transition_valid(states.UNSET, None))
        self.assertTrue(states.is_transition_valid(None, states.UNSET))
        self.assertTrue(states.is_transition_valid(states.UNSET, states.UNSET))

    def test_transition(self):
        for x, y in [(x, y) for x in states.ALL_STATES for y in states.ALL_STATES]:
            expected = (x == y or y in states.VALID_STATE_TRANSITION_MAP[x])
            self.assertEqual(states.is_transition_valid(x, y), expected)
