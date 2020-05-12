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

from orquesta.tests.unit import base as test_base


class JinjaVariableExtractionTest(test_base.ExpressionEvaluatorTest):
    @classmethod
    def setUpClass(cls):
        cls.language = "jinja"
        super(JinjaVariableExtractionTest, cls).setUpClass()

    def test_empty_extraction(self):
        expr = "{{ just_text and $not_a_var and notctx().bar }}"

        self.assertListEqual([], self.evaluator.extract_vars(expr))

    def test_single_var_extraction(self):
        expr = "{{ ctx().foobar }}"

        expected_vars = ["ctx().foobar"]

        self.assertListEqual(expected_vars, self.evaluator.extract_vars(expr))

    def test_single_dotted_var_extraction(self):
        expr = "{{ ctx().foo.bar }}"

        expected_vars = ["ctx().foo.bar"]

        self.assertListEqual(expected_vars, self.evaluator.extract_vars(expr))

    def test_single_indexing_var_extraction(self):
        expr = "{{ ctx().foo[0] }}"

        expected_vars = ["ctx().foo[0]"]

        self.assertListEqual(expected_vars, self.evaluator.extract_vars(expr))

    def test_single_functional_var_extraction(self):
        expr = "{{ ctx().foo.get(bar) }}"

        expected_vars = ["ctx().foo.get(bar)"]

        self.assertListEqual(expected_vars, self.evaluator.extract_vars(expr))

    def test_multiple_vars_extraction(self):
        expr = "{{ctx().fubar ctx().foobar ctx().foo.get(bar) ctx().fu.bar ctx().foobaz.bar[0] }}"

        expected_vars = [
            "ctx().foobar",
            "ctx().foobaz.bar[0]",
            "ctx().foo.get(bar)",
            "ctx().fubar",
            "ctx().fu.bar",
        ]

        self.assertListEqual(sorted(expected_vars), sorted(self.evaluator.extract_vars(expr)))

    def test_multiple_interleaved_vars_extraction(self):
        expr = "{{ Why the ctx().foobar are you so ctx().fu.bar serious? }}"

        expected_vars = ["ctx().foobar", "ctx().fu.bar"]

        self.assertListEqual(expected_vars, sorted(self.evaluator.extract_vars(expr)))
