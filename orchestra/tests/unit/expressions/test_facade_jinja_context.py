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


class JinjaFacadeVariableExtractionTest(base.ExpressionFacadeEvaluatorTest):

    def test_empty_extraction(self):
        expr = '{{ just_text and _not_a_var }}'

        self.assertListEqual([], expressions.extract_vars(expr))

    def test_single_var_extraction(self):
        expr = '{{ _.foobar  }}'

        expected_vars = [('jinja', expr, 'foobar')]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_dotted_var_extraction(self):
        expr = '{{ _.foo.bar  }}'

        expected_vars = [('jinja', expr, 'foo')]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_indexing_var_extraction(self):
        expr = '{{ _.foo[0]  }}'

        expected_vars = [('jinja', expr, 'foo')]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_single_functional_var_extraction(self):
        expr = '{{ _.foo.get(bar)  }}'

        expected_vars = [('jinja', expr, 'foo')]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_multiple_vars_extraction(self):
        expr = '{{ _.foobar _.foo.get(bar) _.fu.bar _.fu.bar[0]  }}'

        expected_vars = [
            ('jinja', expr, 'foo'),
            ('jinja', expr, 'foobar'),
            ('jinja', expr, 'fu')
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_multiple_interleaved_vars_extraction(self):
        expr = '{{ Why the _.foobar are you so _.fu.bar serious? }}'

        expected_vars = [
            ('jinja', expr, 'foobar'),
            ('jinja', expr, 'fu')
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_vars_extraction_from_list(self):
        expr = [
            '{{ abc }}',
            '{{ Why the _.foobar are you so _.fu.bar serious? }}',
            'All your base are belong to us.',
            {'{{ _.x }}': 123, 'k2': '{{ _.y }}', 'k3': ['{{ _.z }}']}
        ]

        expected_vars = [
            ('jinja', expr[1], 'foobar'),
            ('jinja', expr[1], 'fu'),
            ('jinja', '{{ _.x }}', 'x'),
            ('jinja', '{{ _.y }}', 'y'),
            ('jinja', '{{ _.z }}', 'z')
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))

    def test_vars_extraction_from_dict(self):
        expr = {
            'k1': '{{ abc }}',
            'k2': '{{ Why the _.foobar are you so _.fu.bar serious? }}',
            'k3': ['{{ _.z }}'],
            'k4': {'k5': '{{ _.y }}'},
            '{{ _.x }}': 123
        }

        expected_vars = [
            ('jinja', expr['k2'], 'foobar'),
            ('jinja', expr['k2'], 'fu'),
            ('jinja', '{{ _.x }}', 'x'),
            ('jinja', '{{ _.y }}', 'y'),
            ('jinja', '{{ _.z }}', 'z')
        ]

        self.assertListEqual(expected_vars, expressions.extract_vars(expr))
