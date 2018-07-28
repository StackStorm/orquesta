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

import copy
import unittest

from orquesta.utils import context as ctx


class ContextUtilTest(unittest.TestCase):

    def test_set_current_task(self):
        context = {'var1': 'foobar'}
        task = {'id': 't1_1', 'name': 't1'}

        context = ctx.set_current_task(context, task)
        expected_context = dict([('__current_task', copy.deepcopy(task))] + list(context.items()))

        self.assertDictEqual(context, expected_context)

    def test_set_current_task_with_result(self):
        context = {'var1': 'foobar'}
        task = {'id': 't1_1', 'name': 't1', 'result': 'foobar'}

        context = ctx.set_current_task(context, task)
        expected_context = dict([('__current_task', copy.deepcopy(task))] + list(context.items()))

        self.assertDictEqual(context, expected_context)

    def test_set_current_task_nonetype_context(self):
        task = {'id': 't1_1', 'name': 't1'}

        context = ctx.set_current_task(None, task)
        expected_context = {'__current_task': copy.deepcopy(task)}

        self.assertDictEqual(context, expected_context)

    def test_set_current_task_empty_context(self):
        task = {'id': 't1_1', 'name': 't1'}

        context = ctx.set_current_task(dict(), task)
        expected_context = {'__current_task': copy.deepcopy(task)}

        self.assertDictEqual(context, expected_context)

    def test_set_current_task_empty_task(self):
        context = {'var1': 'foobar'}

        self.assertRaises(ValueError, ctx.set_current_task, context, dict())

    def test_set_current_task_bad_types(self):
        task = {'id': 't1_1', 'name': 't1'}

        self.assertRaises(TypeError, ctx.set_current_task, 'foobar', task)

        self.assertRaises(TypeError, ctx.set_current_task, dict(), 'foobar')
