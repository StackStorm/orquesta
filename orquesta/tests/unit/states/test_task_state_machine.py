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
from orquesta import statuses


class MockExecutionEvent(events.ActionExecutionEvent):

    def __init__(self, name, status):
        self.name = name
        self.status = status


class TaskStateMachineTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TaskStateMachineTest, cls).setUpClass()
        statuses.ALL_STATUSES.append('mock')

    @classmethod
    def tearDownClass(cls):
        statuses.ALL_STATUSES.remove('mock')
        super(TaskStateMachineTest, cls).tearDownClass()

    def test_bad_event_name(self):
        task_flow_entry = {'id': 'task1', 'route': 0, 'ctx': 0}
        mock_event = MockExecutionEvent('foobar', statuses.RUNNING)

        self.assertRaises(
            exc.InvalidEvent,
            machines.TaskStateMachine.process_event,
            None,
            task_flow_entry,
            mock_event
        )

    def test_bad_event_status(self):
        self.assertRaises(
            exc.InvalidStatus,
            events.ExecutionEvent,
            'mock_event',
            'foobar'
        )

    def test_bad_current_task_status(self):
        task_flow_entry = {'id': 'task1', 'route': 0, 'ctx': 0, 'status': 'mock'}
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)

        self.assertRaises(
            exc.InvalidTaskStatusTransition,
            machines.TaskStateMachine.process_event,
            None,
            task_flow_entry,
            ac_ex_event
        )

    def test_bad_current_task_status_to_event_mapping(self):
        task_flow_entry = {'id': 'task1', 'route': 0, 'ctx': 0, 'status': statuses.REQUESTED}
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
        transition_ev = machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertIsNone(transition_ev)
        self.assertEqual(task_flow_entry['status'], statuses.REQUESTED)

    def test_current_task_status_unset(self):
        task_flow_entry = {'id': 'task1', 'route': 0, 'ctx': 0}
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        transition_ev = machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(transition_ev, events.ACTION_RUNNING)
        self.assertEqual(task_flow_entry['status'], statuses.RUNNING)

    def test_current_task_status_none(self):
        task_flow_entry = {'id': 'task1', 'route': 0, 'ctx': 0, 'status': None}
        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
        transition_ev = machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(transition_ev, events.ACTION_RUNNING)
        self.assertEqual(task_flow_entry['status'], statuses.RUNNING)

    def test_task_status_transition(self):
        task_flow_entry = {'id': 'task1', 'route': 0, 'ctx': 0, 'status': statuses.RUNNING}
        ac_ex_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
        transition_ev = machines.TaskStateMachine.process_event(None, task_flow_entry, ac_ex_event)
        self.assertEqual(transition_ev, events.ACTION_SUCCEEDED)
        self.assertEqual(task_flow_entry['status'], statuses.SUCCEEDED)


class FailedStateTransitionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(FailedStateTransitionTest, cls).setUpClass()
        statuses.ALL_STATUSES.append('mock')

    @classmethod
    def tearDownClass(cls):
        statuses.ALL_STATUSES.remove('mock')
        super(FailedStateTransitionTest, cls).tearDownClass()

    def test_invalid_old_status(self):
        self.assertRaises(
            exc.InvalidStatus,
            machines.TaskStateMachine.is_transition_valid,
            'foobar',
            statuses.REQUESTED
        )

    def test_invalid_new_status(self):
        self.assertRaises(
            exc.InvalidStatus,
            machines.TaskStateMachine.is_transition_valid,
            statuses.UNSET,
            'foobar'
        )

    def test_original_status_not_in_transition_map(self):
        self.assertFalse(machines.TaskStateMachine.is_transition_valid('mock', None))

    def test_invalid_transition(self):
        self.assertFalse(machines.TaskStateMachine.is_transition_valid(
            statuses.FAILED,
            statuses.SUCCEEDED
        ))


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
            expected = (x == y or y in machines.TASK_STATE_MACHINE_DATA[x].values())
            self.assertEqual(machines.TaskStateMachine.is_transition_valid(x, y), expected)


class RetryStateTransitionTest(unittest.TestCase):

    def setUp(self):
        self.workflow_state = conducting.WorkflowState()
        self.task_flow_entry = {'id': 'task', 'route': 0, 'ctxs': {'in': [0]}}

        # initialize workflow_state instance to be able to work state transition processing propery
        self.workflow_state.tasks['task__r0'] = 0
        self.workflow_state.sequence.append(self.task_flow_entry)
        self.workflow_state.contexts.append({})

    def assert_retry_task(self, case):
        self.task_flow_entry['status'] = statuses.RUNNING
        self.task_flow_entry['retry_count'] = case['retry_count']
        self.task_flow_entry['retry_condition'] = case['retry_condition']

        ac_ex_event = events.ActionExecutionEvent(case['action_status'])
        event = machines.TaskStateMachine.process_event(self.workflow_state,
                                                        self.task_flow_entry,
                                                        ac_ex_event)

        self.assertEqual(event, case['expected_event'])
        self.assertEqual(self.task_flow_entry['status'], case['expected_status'])

    def test_task_will_be_retry(self):
        # This defines each task state and event conditions that tasks will be retried through
        # all state transition about retrying.
        test_cases = [
            {
                'retry_count': 1, 'retry_condition': '<% failed() %>',
                'action_status': statuses.FAILED,
                'expected_event': events.ACTION_FAILED_RETRYING,
                'expected_status': statuses.RUNNING
            }, {
                'retry_count': 1, 'retry_condition': '<% succeeded() %>',
                'action_status': statuses.SUCCEEDED,
                'expected_event': events.ACTION_SUCCEEDED_RETRYING,
                'expected_status': statuses.RUNNING
            }, {
                'retry_count': 1, 'retry_condition': '<% completed() %>',
                'action_status': statuses.EXPIRED,
                'expected_event': events.ACTION_EXPIRED_RETRYING,
                'expected_status': statuses.RUNNING
            }, {
                'retry_count': 1, 'retry_condition': '<% completed() %>',
                'action_status': statuses.ABANDONED,
                'expected_event': events.ACTION_ABANDONED_RETRYING,
                'expected_status': statuses.RUNNING
            }, {
                'retry_count': 1, 'retry_condition': '<% "foo" = "foo" %>',
                'action_status': statuses.FAILED,
                'expected_event': events.ACTION_FAILED_RETRYING,
                'expected_status': statuses.RUNNING
            }
        ]
        for case in test_cases:
            self.assert_retry_task(case)

    def test_task_will_not_be_retryed(self):
        # This defines each task state and event conditions that tasks won't be retried.
        test_cases = [
            {
                # Task won't be retried because retry condition isn't satisfied.
                'retry_count': 1, 'retry_condition': '<% failed() %>',
                'action_status': statuses.SUCCEEDED,
                'expected_event': events.ACTION_SUCCEEDED,
                'expected_status': statuses.SUCCEEDED
            }, {
                # Task won't be retried because retry condition isn't satisfied.
                'retry_count': 1, 'retry_condition': '<% succeeded() %>',
                'action_status': statuses.FAILED,
                'expected_event': events.ACTION_FAILED,
                'expected_status': statuses.FAILED
            }, {
                # Task won't be retried because retry_count doesn't left
                'retry_count': 0, 'retry_condition': '<% failed() %>',
                'action_status': statuses.FAILED,
                'expected_event': events.ACTION_FAILED,
                'expected_status': statuses.FAILED
            }, {
                # Task won't be retried because retry_count doesn't left
                'retry_count': 0, 'retry_condition': '<% succeeded() %>',
                'action_status': statuses.SUCCEEDED,
                'expected_event': events.ACTION_SUCCEEDED,
                'expected_status': statuses.SUCCEEDED
            }, {
                # Below cases confirm retry parameter doesn't affect for other events
                'retry_count': 1, 'retry_condition': '<% completed() %>',
                'action_status': statuses.RUNNING,
                'expected_event': events.ACTION_RUNNING,
                'expected_status': statuses.RUNNING
            }, {
                'retry_count': 1, 'retry_condition': '<% completed() %>',
                'action_status': statuses.PENDING,
                'expected_event': events.ACTION_PENDING,
                'expected_status': statuses.PENDING
            }, {
                'retry_count': 1, 'retry_condition': '<% completed() %>',
                'action_status': statuses.PAUSED,
                'expected_event': events.ACTION_PAUSED,
                'expected_status': statuses.PAUSED
            }, {
                'retry_count': 1, 'retry_condition': '<% completed() %>',
                'action_status': statuses.CANCELING,
                'expected_event': events.ACTION_CANCELING,
                'expected_status': statuses.CANCELING
            }, {
                'retry_count': 1, 'retry_condition': '<% completed() %>',
                'action_status': statuses.CANCELED,
                'expected_event': events.ACTION_CANCELED,
                'expected_status': statuses.CANCELED
            }, {
                'retry_count': 1, 'retry_condition': '<% "foo" = "bar" %>',
                'action_status': statuses.FAILED,
                'expected_event': events.ACTION_FAILED,
                'expected_status': statuses.FAILED
            }
        ]
        for case in test_cases:
            self.assert_retry_task(case)
