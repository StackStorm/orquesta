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


class JinjaValidationTest(test_base.ExpressionEvaluatorTest):
    @classmethod
    def setUpClass(cls):
        cls.language = "jinja"
        super(JinjaValidationTest, cls).setUpClass()

    def test_get_evaluator(self):
        self.assertEqual(
            plugin_util.get_module("orquesta.expressions.evaluators", self.language),
            jinja_expr.JinjaEvaluator,
        )

    def test_basic_validate(self):
        self.assertListEqual([], self.evaluator.validate("{{ 1 }}"))
        self.assertListEqual([], self.evaluator.validate("{{ abc }}"))
        self.assertListEqual([], self.evaluator.validate("{{ 1 + 2 }}"))
        self.assertListEqual([], self.evaluator.validate("{{ ctx().foo }}"))
        self.assertListEqual([], self.evaluator.validate('{{ ctx("foo") }}'))
        self.assertListEqual([], self.evaluator.validate('{{ ctx().a1 + ctx("a2") }}'))

    def test_parse_error(self):
        expr = "{{ {{ ctx().foo }} }}"
        errors = self.evaluator.validate(expr)

        self.assertEqual(2, len(errors))
        self.assertIn("expected token ':', got '}'", errors[0]["message"])
        self.assertIn("unexpected end of template", errors[1]["message"])

    def test_multiple_errors(self):
        expr = "{{ 1 +/ 2 }} and {{ * }}"
        errors = self.evaluator.validate(expr)

        self.assertEqual(3, len(errors))
        self.assertIn("unexpected", errors[0]["message"])
        self.assertIn("unexpected", errors[1]["message"])
        self.assertIn("unexpected", errors[2]["message"])

    def test_block_error(self):
        expr = "{% for i in ctx().x %}{{ i }}{% foobar %}"
        errors = self.evaluator.validate(expr)

        self.assertEqual(1, len(errors))
        self.assertIn("unknown tag", errors[0]["message"])

    def test_missing_braces_error(self):
        expr = "{% for i in ctx().x %}{{ i }}{{ foobar %}"
        errors = self.evaluator.validate(expr)

        self.assertEqual(1, len(errors))
        self.assertIn("unexpected '}'", errors[0]["message"])
