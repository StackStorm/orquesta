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

from orquesta import exceptions as exc
from orquesta.expressions.functions import workflow as funcs


class ItemFunctionTest(unittest.TestCase):
    def test_missing_current_item(self):
        self.assertRaises(exc.ExpressionEvaluationException, funcs.item_, {})

    def test_item_is_null(self):
        context = {"__current_item": None}
        self.assertIsNone(funcs.item_(context))

    def test_item(self):
        context = {"__current_item": "foobar"}
        self.assertEqual(funcs.item_(context), "foobar")

    def test_item_is_dict(self):
        context = {"__current_item": {"foo": "bar"}}
        self.assertDictEqual(funcs.item_(context), {"foo": "bar"})

    def test_item_is_not_dict(self):
        context = {"__current_item": "foobar"}
        self.assertRaises(exc.ExpressionEvaluationException, funcs.item_, context, key="foo")

    def test_item_key(self):
        context = {"__current_item": {"foo": "bar"}}
        self.assertEqual(funcs.item_(context, key="foo"), "bar")

    def test_item_bad_key(self):
        context = {"__current_item": {"foo": "bar"}}
        self.assertRaises(exc.ExpressionEvaluationException, funcs.item_, context, key="bar")
