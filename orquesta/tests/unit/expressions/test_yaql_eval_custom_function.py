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

from orquesta.expressions import yql as yaql_expr
from orquesta.tests.unit import base as test_base
from orquesta.utils import plugin as plugin_util


class YAQLEvaluationTest(test_base.ExpressionEvaluatorTest):
    @classmethod
    def setUpClass(cls):
        cls.language = "yaql"
        super(YAQLEvaluationTest, cls).setUpClass()

    def test_get_evaluator(self):
        e = plugin_util.get_module("orquesta.expressions.evaluators", self.language)

        self.assertEqual(e, yaql_expr.YAQLEvaluator)
        self.assertIn("json", e._custom_functions.keys())

    def test_custom_function(self):
        expr = "<% json('{\"a\": 123}') %>"

        self.assertDictEqual({"a": 123}, self.evaluator.evaluate(expr))

    def test_custom_function_failure(self):
        expr = "<% json(int(123)) %>"

        self.assertRaises(yaql_expr.YaqlEvaluationException, self.evaluator.evaluate, expr)
