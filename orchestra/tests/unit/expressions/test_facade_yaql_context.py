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

        expected_vars = [('yaql', expr, 'foobar')]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_dotted_var_extraction(self):
        expr = '<% $.foo.bar  %>'

        expected_vars = [('yaql', expr, 'foo')]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_indexing_var_extraction(self):
        expr = '<% $.foo[0]  %>'

        expected_vars = [('yaql', expr, 'foo')]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_functional_var_extraction(self):
        expr = '<% $.foo.get(bar)  %>'

        expected_vars = [('yaql', expr, 'foo')]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_multiple_vars_extraction(self):
        expr = '<% $.foobar $.foo.get(bar) $.fu.bar $.fu.bar[0]  %>'

        expected_vars = [
            ('yaql', expr, 'foo'),
            ('yaql', expr, 'foobar'),
            ('yaql', expr, 'fu')
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_multiple_interleaved_vars_extraction(self):
        expr = '<% Why the $.foobar are you so $.fu.bar serious? %>'

        expected_vars = [
            ('yaql', expr, 'foobar'),
            ('yaql', expr, 'fu')
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_vars_extraction_from_list(self):
        expr = [
            '<% abc %>',
            '<% Why the $.foobar are you so $.fu.bar serious? %>',
            'All your base are belong to us.',
            {'<% $.x %>': 123, 'k2': '<% $.y %>', 'k3': ['<% $.z %>']}
        ]

        expected_vars = [
            ('yaql', expr[1], 'foobar'),
            ('yaql', expr[1], 'fu'),
            ('yaql', '<% $.x %>', 'x'),
            ('yaql', '<% $.y %>', 'y'),
            ('yaql', '<% $.z %>', 'z')
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_vars_extraction_from_dict(self):
        expr = {
            'k1': '<% abc %>',
            'k2': '<% Why the $.foobar are you so $.fu.bar serious? %>',
            'k3': ['<% $.z %>'],
            'k4': {'k5': '<% $.y %>'},
            '<% $.x %>': 123
        }

        expected_vars = [
            ('yaql', expr['k2'], 'foobar'),
            ('yaql', expr['k2'], 'fu'),
            ('yaql', '<% $.x %>', 'x'),
            ('yaql', '<% $.y %>', 'y'),
            ('yaql', '<% $.z %>', 'z')
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))
