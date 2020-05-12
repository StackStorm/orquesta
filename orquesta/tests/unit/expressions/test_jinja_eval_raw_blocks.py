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
        self.assertIn("task_status", e._custom_functions.keys())

    def test_block_eval(self):
        expr = "{% for i in ctx().x %}{{ i }}{% endfor %}"

        data = {"x": ["a", "b", "c"]}

        self.assertEqual("abc", self.evaluator.evaluate(expr, data))

    def test_block_eval_complex_data(self):
        expr = "{% for i in ctx().x %}{{ i.k }}{{ ctx().z }}{% endfor %}"

        data = {"x": [{"k": "a"}, {"k": "b"}, {"k": "c"}], "z": "->"}

        self.assertEqual("a->b->c->", self.evaluator.evaluate(expr, data))

    def test_block_eval_undefined(self):
        expr = "{% for i in ctx().x %}{{ ctx().y }}{% endfor %}"

        data = {"x": ["a", "b", "c"]}

        self.assertRaises(jinja_expr.JinjaEvaluationException, self.evaluator.evaluate, expr, data)

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

        self.assertEqual("abcdef", self.evaluator.evaluate(expr, data))

    def test_multi_block_eval(self):
        expr = (
            "{% for i in ctx().x %}{{ i }}{% endfor %}" "{% for i in ctx().y %}{{ i }}{% endfor %}"
        )

        data = {"x": ["a", "b", "c"], "y": ["d", "e", "f"]}

        self.assertEqual("abcdef", self.evaluator.evaluate(expr, data))

    def test_raw_block(self):
        expr = "{% raw %}{{ ctx().foo }}{% endraw %}"

        data = {"foo": "bar"}

        self.assertEqual("{{ ctx().foo }}", self.evaluator.evaluate(expr, data))

    def test_multi_raw_blocks(self):
        expr = (
            "{% raw %}{{ ctx().foo }}{% endraw %} and "
            "{% raw %}{{ ctx().bar }}{% endraw %} and "
            "{% raw %}foobar{% endraw %}"
        )

        data = {"foo": "bar"}

        self.assertEqual(
            "{{ ctx().foo }} and {{ ctx().bar }} and foobar", self.evaluator.evaluate(expr, data)
        )

    def test_mix_block_and_expr_eval(self):
        expr = (
            "{{ ctx().a }}{% for i in ctx().x %}{{ i }}{% endfor %}{{ ctx().d }} and "
            "{% raw %}{{ 1234 }}{% endraw %}"
        )

        data = {"x": ["b", "c"], "a": "a", "d": "d"}

        self.assertEqual("abcd and {{ 1234 }}", self.evaluator.evaluate(expr, data))
