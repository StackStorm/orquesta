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
from orquesta import constants
from orquesta import statuses
from orquesta.utils import task_state as task_state_util

import unittest


class TaskStateUtilTest(unittest.TestCase):

    def setUp(self):
        # This creates common datastructures which are WorkflowState instance
        # and task_state object.
        self.workflow_state = conducting.WorkflowState()
        self.task_state = {
            'id': 'task1',
            'route': 0,
            'ctxs': {'in': [0]},
            'status': statuses.UNSET
        }

        # initialize workflow_state object to be able to evaluate task status
        # in the expression function correctly.
        self.workflow_state.tasks[constants.TASK_STATE_ROUTE_FORMAT % ('task1', str(0))] = 0
        self.workflow_state.sequence.append(self.task_state)
        self.workflow_state.contexts.append({})

    def assertRetryTask(self, result):
        self.assertTrue(task_state_util.will_retry(self.task_state, self.workflow_state, result))

    def assertNotRetryTask(self, result):
        self.assertFalse(task_state_util.will_retry(self.task_state, self.workflow_state, result))

    def test_will_be_retried(self):
        # These are the cases that task will be retried
        test_patterns = [
            {'retry_count': 1, 'retry_cond': '<% failed() %>', 'status': statuses.FAILED},
            {'retry_count': 1, 'retry_cond': '<% succeeded() %>', 'status': statuses.SUCCEEDED},
            {'retry_count': 1, 'retry_cond': '<% completed() %>', 'status': statuses.SUCCEEDED},
            {'retry_count': 1, 'retry_cond': '<% completed() %>', 'status': statuses.FAILED},
            {'retry_count': 1, 'retry_cond': '<% completed() %>', 'status': statuses.EXPIRED},
            {'retry_count': 1, 'retry_cond': '<% completed() %>', 'status': statuses.ABANDONED},
            {'retry_count': 1, 'retry_cond': '<% failed() or result().status_code = 401 %>',
                'status': statuses.SUCCEEDED, 'result': {'status_code': 401}},
            {'retry_count': 1, 'retry_cond': '<% failed() or result().status_code = 401 %>',
                'status': statuses.FAILED, 'result': {'status_code': 500}},
        ]
        for info in test_patterns:
            self.task_state['status'] = info['status']
            self.task_state['retry_count'] = info['retry_count']
            self.task_state['retry_condition'] = info['retry_cond']

            self.assertRetryTask(info['result'] if 'result' in info else [])

    def test_will_not_be_retried(self):
        # These are the cases that task won't be retried
        test_patterns = [
            {'retry_count': 0, 'retry_cond': '<% failed() %>', 'status': statuses.FAILED},
            {'retry_count': 1, 'retry_cond': '<% failed() %>', 'status': statuses.SUCCEEDED},
            {'retry_count': 1, 'retry_cond': '<% completed() %>', 'status': statuses.CANCELED},
            {'retry_count': 1, 'retry_cond': '<% failed() or result().status_code = 401 %>',
                'status': statuses.SUCCEEDED, 'result': {'status_code': 200}},
        ]
        for info in test_patterns:
            self.task_state['status'] = info['status']
            self.task_state['retry_count'] = info['retry_count']
            self.task_state['retry_condition'] = info['retry_cond']

            self.assertNotRetryTask(info['result'] if 'result' in info else [])

    def test_specify_invalid_task_state(self):
        with self.assertRaises(TypeError, msg='The task_state parameter must be dict type'):
            task_state_util.will_retry(None, self.workflow_state)

    def test_specify_invalid_workflow_state(self):
        self.assertFalse(task_state_util.will_retry(self.task_state, None))
