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

from orquesta import constants
from orquesta import exceptions as exc
from orquesta.expressions.functions import workflow as funcs
from orquesta import states


class WorkflowFunctionTest(unittest.TestCase):

    def test_task_state_empty_context(self):
        task_route = 0
        task_name = 't1'
        task_flow_pointer_id = constants.TASK_FLOW_ROUTE_FORMAT % (task_name, str(task_route))
        task_flow_entry = {'id': task_name, 'route': task_route}

        context = None
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.UNSET)

        context = {}
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.UNSET)

        context = {'__flow': None}
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.UNSET)

        context = {'__flow': {}}
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.UNSET)

        context = {'__flow': {'tasks': None}}
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.UNSET)

        context = {'__flow': {'tasks': {}}}
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.UNSET)

        task_pointers = {task_flow_pointer_id: None}
        context = {'__flow': {'tasks': task_pointers}}
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.UNSET)

        task_pointers = {task_flow_pointer_id: 0}
        context = {'__flow': {'tasks': task_pointers, 'sequence': [None]}}
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.UNSET)

    def test_task_state_dereference_errors(self):
        task_route = 0
        task_name = 't1'
        task_flow_pointer_id = constants.TASK_FLOW_ROUTE_FORMAT % (task_name, str(task_route))
        task_flow_entry = {'id': task_name, 'route': task_route}

        task_pointers = {task_flow_pointer_id: 0}
        context = {'__flow': {'tasks': task_pointers}}
        self.assertRaises(IndexError, funcs.task_state_, context, task_flow_entry)

        task_pointers = {task_flow_pointer_id: 1}
        context = {'__flow': {'tasks': task_pointers, 'sequence': [{'state': states.RUNNING}]}}
        self.assertRaises(IndexError, funcs.task_state_, context, task_flow_entry)

    def test_task_state(self):
        task_route = 0
        task_name = 't1'
        task_flow_pointer_id = constants.TASK_FLOW_ROUTE_FORMAT % (task_name, str(task_route))
        task_flow_entry = {'id': task_name, 'route': task_route}

        task_pointers = {task_flow_pointer_id: 0}
        context = {'__flow': {'tasks': task_pointers, 'sequence': [{'state': states.RUNNING}]}}
        self.assertEqual(funcs.task_state_(context, task_flow_entry), states.RUNNING)

    def test_get_current_task_empty(self):
        self.assertRaises(exc.ExpressionEvaluationException, funcs._get_current_task, None)

        context = {'__current_task': None}
        self.assertRaises(exc.ExpressionEvaluationException, funcs._get_current_task, context)

        context = {'__current_task': {}}
        self.assertRaises(exc.ExpressionEvaluationException, funcs._get_current_task, context)

    def test_get_current_task(self):
        context = {'__current_task': {'id': 't1', 'route': 0}}
        self.assertDictEqual(funcs._get_current_task(context), context['__current_task'])

    def test_succeeded(self):
        task_route = 0
        task_name = 't1'
        task_flow_pointer_id = constants.TASK_FLOW_ROUTE_FORMAT % (task_name, str(task_route))

        context = {'__current_task': None}
        self.assertRaises(exc.ExpressionEvaluationException, funcs.succeeded_, context)

        context = {
            '__current_task': {'id': task_name, 'route': task_route},
            '__flow': {
                'tasks': {task_flow_pointer_id: 0},
                'sequence': [{'state': states.RUNNING}]
            }
        }

        self.assertFalse(funcs.succeeded_(context))

        context = {
            '__current_task': {'id': task_name, 'route': task_route},
            '__flow': {
                'tasks': {task_flow_pointer_id: 0},
                'sequence': [{'state': states.SUCCEEDED}]
            }
        }

        self.assertTrue(funcs.succeeded_(context))

    def test_failed(self):
        task_route = 0
        task_name = 't1'
        task_flow_pointer_id = constants.TASK_FLOW_ROUTE_FORMAT % (task_name, str(task_route))

        context = {'__current_task': None}
        self.assertRaises(exc.ExpressionEvaluationException, funcs.failed_, context)

        context = {
            '__current_task': {'id': task_name, 'route': task_route},
            '__flow': {
                'tasks': {task_flow_pointer_id: 0},
                'sequence': [{'state': states.RUNNING}]
            }
        }

        self.assertFalse(funcs.failed_(context))

        context = {
            '__current_task': {'id': task_name, 'route': task_route},
            '__flow': {
                'tasks': {task_flow_pointer_id: 0},
                'sequence': [{'state': states.FAILED}]
            }
        }

        self.assertTrue(funcs.failed_(context))

    def test_completed(self):
        task_route = 0
        task_name = 't1'
        task_flow_pointer_id = constants.TASK_FLOW_ROUTE_FORMAT % (task_name, str(task_route))

        context = {'__current_task': None}
        self.assertRaises(exc.ExpressionEvaluationException, funcs.completed_, context)

        context = {
            '__current_task': {'id': task_name, 'route': task_route},
            '__flow': {
                'tasks': {task_flow_pointer_id: 0},
                'sequence': [{'state': states.RUNNING}]
            }
        }

        self.assertFalse(funcs.completed_(context))

        context = {
            '__current_task': {'id': task_name, 'route': task_route},
            '__flow': {
                'tasks': {task_flow_pointer_id: 0},
                'sequence': [{'state': states.SUCCEEDED}]
            }
        }

        self.assertTrue(funcs.completed_(context))

    def test_result(self):
        task_route = 0
        task_name = 't1'
        task_flow_pointer_id = constants.TASK_FLOW_ROUTE_FORMAT % (task_name, str(task_route))

        context = {'__current_task': None}
        self.assertRaises(exc.ExpressionEvaluationException, funcs.result_, context)

        context = {
            '__current_task': {'id': task_name, 'route': task_route, 'result': 'foobar'},
            '__flow': {
                'tasks': {task_flow_pointer_id: 0},
                'sequence': [{'state': states.SUCCEEDED}]
            }
        }

        self.assertEqual(funcs.result_(context), 'foobar')

        context = {
            '__current_task': {'id': task_name, 'route': task_route, 'result': {'fu': 'bar'}},
            '__flow': {
                'tasks': {task_flow_pointer_id: 0},
                'sequence': [{'state': states.SUCCEEDED}]
            }
        }

        self.assertDictEqual(funcs.result_(context), {'fu': 'bar'})
