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
from orquesta.expressions import base as expr_base


class JinjaFacadeEvaluationTest(unittest.TestCase):
    def test_basic_eval(self):
        data = {"foo": "bar"}

        self.assertEqual("bar", expr_base.evaluate("{{ ctx().foo }}", data))
        self.assertEqual("foobar", expr_base.evaluate("foo{{ ctx().foo }}", data))

    def test_basic_eval_undefined(self):
        expr = "{{ ctx().foo }}"

        data = {}

        self.assertRaises(exc.ExpressionEvaluationException, expr_base.evaluate, expr, data)

    def test_dict_eval(self):
        expr = "{{ ctx().nested.foo }}"

        data = {"nested": {"foo": "bar"}}

        self.assertEqual("bar", expr_base.evaluate(expr, data))

    def test_multi_eval(self):
        expr = "{{ ctx().foo }} and {{ ctx().marco }}"

        data = {"foo": "bar", "marco": "polo"}

        self.assertEqual("bar and polo", expr_base.evaluate(expr, data))

    def test_eval_recursive(self):
        expr = "{{ ctx().fee }}"

        data = {
            "fee": "{{ ctx().fi }}",
            "fi": "{{ ctx().fo }}",
            "fo": "{{ ctx().fum }}",
            "fum": "fee-fi-fo-fum",
        }

        self.assertEqual("fee-fi-fo-fum", expr_base.evaluate(expr, data))

    def test_eval_recursive_undefined(self):
        expr = "{{ ctx().fee }}"

        data = {"fee": "{{ ctx().fi }}", "fi": "{{ ctx().fo }}", "fo": "{{ ctx().fum }}"}

        self.assertRaises(exc.ExpressionEvaluationException, expr_base.evaluate, expr, data)

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

        self.assertEqual("fee-fi-fo-fum! i'm hungry!", expr_base.evaluate(expr, data))

    def test_eval_list(self):
        expr = ["{{ ctx().foo }}", "{{ ctx().marco }}", "foo{{ ctx().foo }}"]

        data = {"foo": "bar", "marco": "polo"}

        self.assertListEqual(["bar", "polo", "foobar"], expr_base.evaluate(expr, data))

    def test_eval_list_of_list(self):
        expr = [["{{ ctx().foo }}", "{{ ctx().marco }}"]]

        data = {"foo": "bar", "marco": "polo"}

        expected = [["bar", "polo"]]

        self.assertListEqual(expected, expr_base.evaluate(expr, data))

    def test_eval_list_of_dict(self):
        expr = [{"foo": "{{ ctx().bar }}", "{{ ctx().marco }}": "polo"}]

        data = {"bar": "bar", "marco": "marco"}

        expected = [{"foo": "bar", "marco": "polo"}]

        self.assertListEqual(expected, expr_base.evaluate(expr, data))

    def test_eval_dict(self):
        expr = {
            "foo": "{{ ctx().bar }}",
            "{{ ctx().marco }}": "polo",
            "foobar": "foo{{ ctx().bar }}",
        }

        data = {"bar": "bar", "marco": "marco"}

        expected = {"foo": "bar", "marco": "polo", "foobar": "foobar"}

        self.assertDictEqual(expected, expr_base.evaluate(expr, data))

    def test_eval_dict_of_dict(self):
        expr = {"nested": {"foo": "{{ ctx().bar }}", "{{ ctx().marco }}": "polo"}}

        data = {"bar": "bar", "marco": "marco"}

        expected = {"nested": {"foo": "bar", "marco": "polo"}}

        self.assertDictEqual(expected, expr_base.evaluate(expr, data))

    def test_eval_dict_of_list(self):
        expr = {"nested": ["{{ ctx().foo }}", "{{ ctx().marco }}"]}

        data = {"foo": "bar", "marco": "polo"}

        expected = {"nested": ["bar", "polo"]}

        self.assertDictEqual(expected, expr_base.evaluate(expr, data))

    def test_type_preservation(self):
        data = {"k1": 101, "k2": 1.999, "k3": True, "k4": [1, 2], "k5": {"k": "v"}, "k6": None}

        self.assertEqual(data["k1"], expr_base.evaluate("{{ ctx().k1 }}", data))

        self.assertEqual(data["k2"], expr_base.evaluate("{{ ctx().k2 }}", data))

        self.assertTrue(expr_base.evaluate("{{ ctx().k3 }}", data))

        self.assertListEqual(data["k4"], expr_base.evaluate("{{ ctx().k4 }}", data))

        self.assertDictEqual(data["k5"], expr_base.evaluate("{{ ctx().k5 }}", data))

        self.assertIsNone(expr_base.evaluate("{{ ctx().k6 }}", data))

    def test_type_string_detection(self):
        expr = "{{ ctx().foo }} -> {{ ctx().bar }}"

        data = {"foo": 101, "bar": 201}

        self.assertEqual("101 -> 201", expr_base.evaluate(expr, data))

    def test_custom_function(self):
        expr = "{{ json('{\"a\": 123}') }}"

        self.assertDictEqual({"a": 123}, expr_base.evaluate(expr))

    def test_custom_function_failure(self):
        expr = "{{ json(int(123)) }}"

        self.assertRaises(exc.ExpressionEvaluationException, expr_base.evaluate, expr)

    def test_block_eval(self):
        expr = "{% for i in ctx().x %}{{ i }}{% endfor %}"

        data = {"x": ["a", "b", "c"]}

        self.assertEqual("abc", expr_base.evaluate(expr, data))

    def test_block_eval_undefined(self):
        expr = "{% for i in ctx().x %}{{ ctx().y }}{% endfor %}"

        data = {"x": ["a", "b", "c"]}

        self.assertRaises(exc.ExpressionEvaluationException, expr_base.evaluate, expr, data)

    def test_block_eval_recursive(self):
        expr = "{% for i in ctx().x %}{{ i }}{% endfor %}"

        data = {
            "x": [
                "{{ ctx().a }}",
                "{{ ctx().b }}",
                "{{ ctx().c }}" "{% for j in ctx().y %}{{ j }}{% endfor %}",
            ],
            "a": "a",
            "b": "b",
            "c": "c",
            "y": ["d", "e", "f"],
        }

        self.assertEqual("abcdef", expr_base.evaluate(expr, data))

    def test_multi_block_eval(self):
        expr = (
            "{% for i in ctx().x %}{{ i }}{% endfor %}" "{% for i in ctx().y %}{{ i }}{% endfor %}"
        )

        data = {"x": ["a", "b", "c"], "y": ["d", "e", "f"]}

        self.assertEqual("abcdef", expr_base.evaluate(expr, data))

    def test_mix_block_and_expr_eval(self):
        expr = "{{ ctx().a }}{% for i in ctx().x %}{{ i }}{% endfor %}{{ ctx().d }}"

        data = {"x": ["b", "c"], "a": "a", "d": "d"}

        self.assertEqual("abcd", expr_base.evaluate(expr, data))
