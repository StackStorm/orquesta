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

from orquesta.expressions import base as expr_base
from orquesta.expressions.functions import common as core_funcs
from orquesta.expressions import jinja as jinja_expr
from orquesta.expressions import yql as yaql_expr


class ExpressionEvaluatorTest(unittest.TestCase):
    def test_mix_types(self):
        expr = "<% ctx().foo %> and {{ ctx().foo }}"

        expected_errors = [
            {
                "type": None,
                "expression": expr,
                "message": "Expression with multiple types is not supported.",
            }
        ]

        result = expr_base.validate(expr)

        self.assertListEqual(expected_errors, result.get("errors", []))

    def test_inspect_function_has_context_argument(self):
        self.assertTrue(expr_base.func_has_ctx_arg(core_funcs.ctx_))
        self.assertFalse(expr_base.func_has_ctx_arg(core_funcs.json_))

    def test_get_statement_regexes(self):
        expected_data = {
            "jinja": jinja_expr.JinjaEvaluator.get_statement_regex(),
            "yaql": yaql_expr.YAQLEvaluator.get_statement_regex(),
        }

        self.assertDictEqual(expr_base.get_statement_regexes(), expected_data)

    def test_has_expressions(self):
        self.assertTrue(expr_base.has_expressions("<% ctx().foo %> and {{ ctx().foo }}"))
        self.assertTrue(expr_base.has_expressions("foo <% ctx().foo %> bar"))
        self.assertTrue(expr_base.has_expressions("foo {{ ctx().foo }} bar"))
        self.assertFalse(expr_base.has_expressions("foobar"))
