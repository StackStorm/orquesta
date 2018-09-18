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

from orquesta.expressions import base as expressions
from orquesta.expressions.functions import common as functions
from orquesta.expressions import jinja as jinja_exp
from orquesta.expressions import yql as yaql_exp


class ExpressionEvaluatorTest(unittest.TestCase):

    def test_mix_types(self):
        expr = '<% ctx().foo %> and {{ ctx().foo }}'

        expected_errors = [
            {
                'type': None,
                'expression': expr,
                'message': 'Expression with multiple types is not supported.'
            }
        ]

        result = expressions.validate(expr)

        self.assertListEqual(
            expected_errors,
            result.get('errors', [])
        )

    def test_inspect_function_has_context_argument(self):
        self.assertTrue(expressions.func_has_ctx_arg(functions.ctx_))
        self.assertFalse(expressions.func_has_ctx_arg(functions.json_))

    def test_get_statement_regexes(self):
        expected_data = {
            'jinja': jinja_exp.JinjaEvaluator.get_statement_regex(),
            'yaql': yaql_exp.YAQLEvaluator.get_statement_regex()
        }

        self.assertDictEqual(expressions.get_statement_regexes(), expected_data)

    def test_has_expressions(self):
        self.assertTrue(expressions.has_expressions('<% ctx().foo %> and {{ ctx().foo }}'))
        self.assertTrue(expressions.has_expressions('foo <% ctx().foo %> bar'))
        self.assertTrue(expressions.has_expressions('foo {{ ctx().foo }} bar'))
        self.assertFalse(expressions.has_expressions('foobar'))
