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

from orquesta import conducting
from orquesta import events
from orquesta import exceptions as exc
from orquesta import machines
from orquesta.specs import native as native_specs
from orquesta import statuses


class WorkflowStateMachineTest(unittest.TestCase):
    def _prep_conductor(self, status=None):
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

        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)

        if status:
            conductor.request_workflow_status(status)

        return conductor

    def test_bad_event_type(self):
        conductor = self._prep_conductor(statuses.RUNNING)
        tk_ex_event = events.ExecutionEvent("foobar", statuses.RUNNING)
        setattr(tk_ex_event, "task_id", "task1")

        self.assertRaises(
            exc.InvalidEventType,
            machines.WorkflowStateMachine.process_event,
            conductor.workflow_state,
            tk_ex_event,
        )

    def test_bad_event_name(self):
        conductor = self._prep_conductor(statuses.RUNNING)
        tk_ex_event = events.TaskExecutionEvent("task1", 0, statuses.RUNNING)
        setattr(tk_ex_event, "name", "foobar")

        self.assertRaises(
            exc.InvalidEvent,
            machines.WorkflowStateMachine.process_event,
            conductor.workflow_state,
            tk_ex_event,
        )

    def test_bad_event_status(self):
        self.assertRaises(exc.InvalidStatus, events.TaskExecutionEvent, "task1", 0, "foobar")

    def test_bad_current_workflow_status(self):
        conductor = self._prep_conductor()
        conductor.workflow_state.status = statuses.ABANDONED
        tk_ex_event = events.TaskExecutionEvent("task1", 0, statuses.RUNNING)

        self.assertRaises(
            exc.InvalidWorkflowStatusTransition,
            machines.WorkflowStateMachine.process_event,
            conductor.workflow_state,
            tk_ex_event,
        )

    def test_bad_current_workflow_status_to_event_mapping(self):
        conductor = self._prep_conductor(statuses.REQUESTED)
        tk_ex_event = events.TaskExecutionEvent("task1", 0, statuses.RUNNING)

        # If transition is not supported, then workflow status will not change.
        machines.WorkflowStateMachine.process_event(conductor.workflow_state, tk_ex_event)
        self.assertEqual(conductor.get_workflow_status(), statuses.REQUESTED)

    def test_workflow_status_transition(self):
        conductor = self._prep_conductor(statuses.RUNNING)

        tk_ex_event = events.TaskExecutionEvent("task1", 0, statuses.RUNNING)
        machines.WorkflowStateMachine.process_event(conductor.workflow_state, tk_ex_event)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        tk_ex_event = events.TaskExecutionEvent("task1", 0, statuses.PAUSED)
        machines.WorkflowStateMachine.process_event(conductor.workflow_state, tk_ex_event)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)


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
            machines.WorkflowStateMachine.is_transition_valid,
            "foobar",
            statuses.REQUESTED,
        )

    def test_invalid_new_status(self):
        self.assertRaises(
            exc.InvalidStatus,
            machines.WorkflowStateMachine.is_transition_valid,
            statuses.UNSET,
            "foobar",
        )

    def test_original_status_not_in_transition_map(self):
        self.assertFalse(machines.WorkflowStateMachine.is_transition_valid("mock", None))


class StateTransitionTest(unittest.TestCase):
    def test_null_statuses(self):
        is_transition_valid = machines.WorkflowStateMachine.is_transition_valid
        self.assertTrue(is_transition_valid(None, None))
        self.assertTrue(is_transition_valid(statuses.UNSET, None))
        self.assertTrue(is_transition_valid(None, statuses.UNSET))
        self.assertTrue(is_transition_valid(statuses.UNSET, statuses.UNSET))

    def test_transition(self):
        cases = [
            (x, y)
            for x in machines.WORKFLOW_STATE_MACHINE_DATA.keys()
            for y in machines.WORKFLOW_STATE_MACHINE_DATA[x].values()
        ]

        for x, y in cases:
            expected = x == y or y in machines.WORKFLOW_STATE_MACHINE_DATA[x].values()
            self.assertEqual(machines.WorkflowStateMachine.is_transition_valid(x, y), expected)
