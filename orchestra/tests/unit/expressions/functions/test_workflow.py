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
from orchestra.expressions.functions import workflow as funcs
from orchestra import states


class WorkflowFunctionTest(unittest.TestCase):

    def test_task_state_empty_context(self):
        context = None
        self.assertEqual(funcs.task_state_(context, 't1'), states.UNSET)

        context = {}
        self.assertEqual(funcs.task_state_(context, 't1'), states.UNSET)

        context = {'__flow': None}
        self.assertEqual(funcs.task_state_(context, 't1'), states.UNSET)

        context = {'__flow': {}}
        self.assertEqual(funcs.task_state_(context, 't1'), states.UNSET)

        context = {'__flow': {'tasks': None}}
        self.assertEqual(funcs.task_state_(context, 't1'), states.UNSET)

        context = {'__flow': {'tasks': {}}}
        self.assertEqual(funcs.task_state_(context, 't1'), states.UNSET)

        context = {'__flow': {'tasks': {'t1': None}}}
        self.assertEqual(funcs.task_state_(context, 't1'), states.UNSET)

        context = {'__flow': {'tasks': {'t1': 0}, 'sequence': [None]}}
        self.assertEqual(funcs.task_state_(context, 't1'), states.UNSET)

    def test_task_state_dereference_errors(self):
        context = {'__flow': {'tasks': {'t1': 0}}}
        self.assertRaises(IndexError, funcs.task_state_, context, 't1')

        context = {'__flow': {'tasks': {'t1': 1}, 'sequence': [{'state': states.RUNNING}]}}
        self.assertRaises(IndexError, funcs.task_state_, context, 't1')

    def test_task_state(self):
        context = {'__flow': {'tasks': {'t1': 0}, 'sequence': [{'state': states.RUNNING}]}}
        self.assertEqual(funcs.task_state_(context, 't1'), states.RUNNING)

    def test_get_current_task_empty(self):
        self.assertRaises(exc.ContextValueError, funcs._get_current_task, None)

        context = {'__current_task': None}
        self.assertRaises(exc.ContextValueError, funcs._get_current_task, context)

        context = {'__current_task': {}}
        self.assertRaises(exc.ContextValueError, funcs._get_current_task, context)

    def test_get_current_task(self):
        context = {'__current_task': {'name': 't1'}}
        self.assertDictEqual(funcs._get_current_task(context), context['__current_task'])

    def test_succeeded(self):
        context = {'__current_task': None}
        self.assertRaises(exc.ContextValueError, funcs.succeeded_, context)

        context = {
            '__current_task': {'id': 't1'},
            '__flow': {'tasks': {'t1': 0}, 'sequence': [{'state': states.RUNNING}]}
        }

        self.assertFalse(funcs.succeeded_(context))

        context = {
            '__current_task': {'id': 't1'},
            '__flow': {'tasks': {'t1': 0}, 'sequence': [{'state': states.SUCCEEDED}]}
        }

        self.assertTrue(funcs.succeeded_(context))

    def test_failed(self):
        context = {'__current_task': None}
        self.assertRaises(exc.ContextValueError, funcs.failed_, context)

        context = {
            '__current_task': {'id': 't1'},
            '__flow': {'tasks': {'t1': 0}, 'sequence': [{'state': states.RUNNING}]}
        }

        self.assertFalse(funcs.failed_(context))

        context = {
            '__current_task': {'id': 't1'},
            '__flow': {'tasks': {'t1': 0}, 'sequence': [{'state': states.FAILED}]}
        }

        self.assertTrue(funcs.failed_(context))

    def test_completed(self):
        context = {'__current_task': None}
        self.assertRaises(exc.ContextValueError, funcs.completed_, context)

        context = {
            '__current_task': {'id': 't1'},
            '__flow': {'tasks': {'t1': 0}, 'sequence': [{'state': states.RUNNING}]}
        }

        self.assertFalse(funcs.completed_(context))

        context = {
            '__current_task': {'id': 't1'},
            '__flow': {'tasks': {'t1': 0}, 'sequence': [{'state': states.SUCCEEDED}]}
        }

        self.assertTrue(funcs.completed_(context))
