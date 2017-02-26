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

from orchestra.expressions import base as expressions
from orchestra.tests.unit import base


class YAQLFacadeVariableExtractionTest(base.ExpressionFacadeEvaluatorTest):

    def test_empty_extraction(self):
        expr = '<% just_text and $not_a_var %>'

        self.assertListEqual([], expressions.extract_vars(expr))

    def test_single_var_extraction(self):
        expr = '<% $.foobar  %>'

        expected_vars = [
            'foobar'
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_dotted_var_extraction(self):
        expr = '<% $.foo.bar  %>'

        expected_vars = [
            'foo'
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_indexing_var_extraction(self):
        expr = '<% $.foo[0]  %>'

        expected_vars = [
            'foo'
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_functional_var_extraction(self):
        expr = '<% $.foo.get(bar)  %>'

        expected_vars = [
            'foo'
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_multiple_vars_extraction(self):
        expr = '<% $.foobar $.foo.get(bar) $.fu.bar $.fu.bar[0]  %>'

        expected_vars = [
            'foo',
            'foobar',
            'fu'
        ]

        self.assertListEqual(
            sorted(expected_vars),
            sorted(expressions.extract_vars(expr))
        )

    def test_multiple_interleaved_vars_extraction(self):
        expr = '<% Why the $.foobar are you so $.fu.bar serious? %>'

        expected_vars = [
            'foobar',
            'fu'
        ]

        self.assertListEqual(
            expected_vars,
            sorted(expressions.extract_vars(expr))
        )
