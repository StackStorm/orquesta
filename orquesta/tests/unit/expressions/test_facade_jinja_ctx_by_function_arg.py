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

from orquesta.expressions import base as expr_base
from orquesta.tests.unit import base as test_base


class JinjaFacadeVariableExtractionTest(test_base.ExpressionFacadeEvaluatorTest):
    def test_empty_extraction(self):
        expr = (
            "{{ just_text and $not_a_var and "
            "notctx(foo) and notctx(\"bar\") and notctx('fu') "
            'ctx("foo\') and ctx(\'foo") and ctx(foo") and '
            "ctx(\"foo) and ctx(foo') and ctx('foo) and "
            "ctx(-foo) and ctx(\"-bar\") and ctx('-fu') and "
            "ctx(foo.bar) and ctx(\"foo.bar\") and ctx('foo.bar') and "
            "ctx(foo()) and ctx(\"foo()\") and ctx('foo()') }}"
        )

        self.assertListEqual([], expr_base.extract_vars(expr))

    def test_single_var_extraction(self):
        expr = '{{ ctx("foobar") }}'

        expected_vars = [("jinja", expr, "foobar")]

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_single_dotted_var_extraction(self):
        expr = '{{ ctx("foo").bar }}'

        expected_vars = [("jinja", expr, "foo")]

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_single_indexing_var_extraction(self):
        expr = '{{ ctx("foo")[0] }}'

        expected_vars = [("jinja", expr, "foo")]

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_single_functional_var_extraction(self):
        expr = '{{ ctx("foo").get(bar) }}'

        expected_vars = [("jinja", expr, "foo")]

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_multiple_vars_extraction(self):
        expr = (
            '{{ctx("fubar") ctx("foobar") ctx("foo").get(bar) '
            'ctx("fu").bar ctx("foobaz").bar[0] }}'
        )

        expected_vars = [
            ("jinja", expr, "foo"),
            ("jinja", expr, "foobar"),
            ("jinja", expr, "foobaz"),
            ("jinja", expr, "fu"),
            ("jinja", expr, "fubar"),
        ]

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_multiple_interleaved_vars_extraction(self):
        expr = '{{ Why the ctx("foobar") are you so ctx("fu").bar serious? }}'

        expected_vars = [("jinja", expr, "foobar"), ("jinja", expr, "fu")]

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_vars_extraction_from_list(self):
        expr = [
            "{{ abc }}",
            '{{ Why the ctx("foobar") are you so ctx("fu").bar serious? }}',
            "All your test_base are belong to us.",
            {'{{ ctx("x") }}': 123, "k2": '{{ ctx("y") }}', "k3": ['{{ ctx("z") }}']},
        ]

        expected_vars = [
            ("jinja", expr[1], "foobar"),
            ("jinja", expr[1], "fu"),
            ("jinja", '{{ ctx("x") }}', "x"),
            ("jinja", '{{ ctx("y") }}', "y"),
            ("jinja", '{{ ctx("z") }}', "z"),
        ]

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_vars_extraction_from_dict(self):
        expr = {
            "k1": "{{ abc }}",
            "k2": '{{ Why the ctx("foobar") are you so ctx("fu").bar serious? }}',
            "k3": ['{{ ctx("z") }}'],
            "k4": {"k5": '{{ ctx("y") }}'},
            '{{ ctx("x") }}': 123,
        }

        expected_vars = [
            ("jinja", expr["k2"], "foobar"),
            ("jinja", expr["k2"], "fu"),
            ("jinja", '{{ ctx("x") }}', "x"),
            ("jinja", '{{ ctx("y") }}', "y"),
            ("jinja", '{{ ctx("z") }}', "z"),
        ]

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_ignore_ctx_dict_funcs(self):
        expr = '{{ctx().keys() and ctx().values() and ctx().set("b", 3) }}'

        expected_vars = []

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))

    def test_ignore_ctx_get_func_calls(self):
        expr = (
            "{{ctx().get(foo) and ctx().get(bar) and ctx().get(\"fu\") and ctx().get('baz') and "
            'ctx().get(foo, "bar") and ctx().get("fu", "bar") and ctx().get(\'bar\', \'foo\') and '
            'ctx().get("foo\') and ctx().get(\'foo") and ctx().get("foo) and ctx().get(foo") }}'
        )

        expected_vars = []

        self.assertListEqual(expected_vars, expr_base.extract_vars(expr))
