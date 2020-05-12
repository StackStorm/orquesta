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

from orquesta.expressions import jinja as jinja_expr
from orquesta.tests.unit import base as test_base
from orquesta.utils import plugin as plugin_util


class JinjaEvaluationTest(test_base.ExpressionEvaluatorTest):
    @classmethod
    def setUpClass(cls):
        cls.language = "jinja"
        super(JinjaEvaluationTest, cls).setUpClass()

    def test_get_evaluator(self):
        e = plugin_util.get_module("orquesta.expressions.evaluators", self.language)

        self.assertEqual(e, jinja_expr.JinjaEvaluator)
        self.assertIn("ctx", e._custom_functions.keys())

    def test_basic_eval(self):
        expr = "{{ ctx().foo }}"

        data = {"foo": "bar"}

        self.assertEqual("bar", self.evaluator.evaluate(expr, data))

    def test_basic_eval_undefined(self):
        expr = "{{ ctx().foo }}"

        data = {}

        self.assertRaises(jinja_expr.JinjaEvaluationException, self.evaluator.evaluate, expr, data)

    def test_dict_eval(self):
        expr = "{{ ctx().nested.foo }}"

        data = {"nested": {"foo": "bar"}}

        self.assertEqual("bar", self.evaluator.evaluate(expr, data))

    def test_multi_eval(self):
        expr = "{{ ctx().foo }} and {{ ctx().marco }}"

        data = {"foo": "bar", "marco": "polo"}

        self.assertEqual("bar and polo", self.evaluator.evaluate(expr, data))

    def test_eval_recursive(self):
        expr = "{{ ctx().fee }}"

        data = {
            "fee": "{{ ctx().fi }}",
            "fi": "{{ ctx().fo }}",
            "fo": "{{ ctx().fum }}",
            "fum": "fee-fi-fo-fum",
        }

        self.assertEqual("fee-fi-fo-fum", self.evaluator.evaluate(expr, data))

    def test_eval_recursive_undefined(self):
        expr = "{{ ctx().fee }}"

        data = {"fee": "{{ ctx().fi }}", "fi": "{{ ctx().fo }}", "fo": "{{ ctx().fum }}"}

        self.assertRaises(jinja_expr.JinjaEvaluationException, self.evaluator.evaluate, expr, data)

    def test_multi_eval_recursive(self):
        expr = "{{ ctx().fee }} {{ ctx().im }}"

        data = {
            "fee": "{{ ctx().fi }}",
            "fi": "{{ ctx().fo }}",
            "fo": "{{ ctx().fum }}",
            "fum": "fee-fi-fo-fum!",
            "im": "{{ ctx().hungry }}",
            "hungry": "i'm hungry!",
        }

        self.assertEqual("fee-fi-fo-fum! i'm hungry!", self.evaluator.evaluate(expr, data))

    def test_type_preservation(self):
        data = {"k1": 101, "k2": 1.999, "k3": True, "k4": [1, 2], "k5": {"k": "v"}, "k6": None}

        self.assertEqual(data["k1"], self.evaluator.evaluate("{{ ctx().k1 }}", data))

        self.assertEqual(data["k2"], self.evaluator.evaluate("{{ ctx().k2 }}", data))

        self.assertTrue(self.evaluator.evaluate("{{ ctx().k3 }}", data))

        self.assertListEqual(data["k4"], self.evaluator.evaluate("{{ ctx().k4 }}", data))

        self.assertDictEqual(data["k5"], self.evaluator.evaluate("{{ ctx().k5 }}", data))

        self.assertIsNone(self.evaluator.evaluate("{{ ctx().k6 }}", data))

    def test_type_string_detection(self):
        expr = "{{ ctx().foo }} -> {{ ctx().bar }}"

        data = {"foo": 101, "bar": 201}

        self.assertEqual("101 -> 201", self.evaluator.evaluate(expr, data))
