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

from orquesta import conducting
from orquesta import events
from orquesta import exceptions as exc
from orquesta.specs import native as specs
from orquesta import states
from orquesta.states import machines


class WorkflowStateMachineTest(unittest.TestCase):

    def _prep_conductor(self, state=None):
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
            next:
              - when: <% succeeded() %>
                do: task3
          task3:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)

        if state:
            conductor.request_workflow_state(state)

        return conductor

    def test_bad_event_type(self):
        conductor = self._prep_conductor(states.RUNNING)
        tk_ex_event = events.ExecutionEvent('foobar', states.RUNNING)
        setattr(tk_ex_event, 'task_id', 'task1')

        self.assertRaises(
            exc.InvalidEventType,
            machines.WorkflowStateMachine.process_event,
            conductor,
            tk_ex_event
        )

    def test_bad_event_name(self):
        conductor = self._prep_conductor(states.RUNNING)
        tk_ex_event = events.TaskExecutionEvent('task1', states.RUNNING)
        setattr(tk_ex_event, 'name', 'foobar')

        self.assertRaises(
            exc.InvalidEvent,
            machines.WorkflowStateMachine.process_event,
            conductor,
            tk_ex_event
        )

    def test_bad_event_state(self):
        self.assertRaises(
            exc.InvalidState,
            events.TaskExecutionEvent,
            'task1',
            'foobar'
        )

    def test_bad_current_workflow_state(self):
        conductor = self._prep_conductor()
        conductor._workflow_state = states.ABANDONED
        tk_ex_event = events.TaskExecutionEvent('task1', states.RUNNING)

        self.assertRaises(
            exc.InvalidWorkflowStateTransition,
            machines.WorkflowStateMachine.process_event,
            conductor,
            tk_ex_event
        )

    def test_bad_current_workflow_state_to_event_mapping(self):
        conductor = self._prep_conductor(states.REQUESTED)
        tk_ex_event = events.TaskExecutionEvent('task1', states.RUNNING)

        # If transition is not supported, then workflow state will not change.
        machines.WorkflowStateMachine.process_event(conductor, tk_ex_event)
        self.assertEqual(conductor.get_workflow_state(), states.REQUESTED)

    def test_workflow_state_transition(self):
        conductor = self._prep_conductor(states.RUNNING)

        tk_ex_event = events.TaskExecutionEvent('task1', states.RUNNING)
        machines.WorkflowStateMachine.process_event(conductor, tk_ex_event)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        tk_ex_event = events.TaskExecutionEvent('task1', states.PAUSED)
        machines.WorkflowStateMachine.process_event(conductor, tk_ex_event)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)


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
