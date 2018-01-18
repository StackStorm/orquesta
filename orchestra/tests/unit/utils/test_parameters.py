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

from orchestra.utils import parameters as params


class InlineParametersTest(unittest.TestCase):

    def test_parse_basic(self):
        tests = [
            ('x=null', {'x': None}),
            ('x=123', {'x': 123}),
            ('x=true', {'x': True}),
            ('x=false', {'x': False}),
            ('x="abc"', {'x': 'abc'}),
            ("x='abc'", {'x': 'abc'})
        ]

        for s, d in tests:
            self.assertDictEqual(params.parse_inline_params(s), d)

    def test_parse_dictionary(self):
        tests = [
            ('x=\'{\"a\": 1}\'', {'x': {'a': 1}})
        ]

        for s, d in tests:
            self.assertDictEqual(params.parse_inline_params(s), d)

    def test_parse_expression(self):
        tests = [
            ('x=<% $.abc %>', {'x': '<% $.abc %>'}),
            ('x={{ _.abc }}', {'x': '{{ _.abc }}'})
        ]

        for s, d in tests:
            self.assertDictEqual(params.parse_inline_params(s), d)

    def test_parse_multiple(self):
        tests = [
            ('x="abc" y="def"', {'x': 'abc', 'y': 'def'}),
            ('x="abc", y="def"', {'x': 'abc', 'y': 'def'}),
            ('x="abc"; y="def"', {'x': 'abc', 'y': 'def'})
        ]

        for s, d in tests:
            self.assertDictEqual(params.parse_inline_params(s), d)

    def test_parse_combination(self):
        s = 'i=123 j="abc" k=true x=<% $.abc %> y={{ _.abc }} z=\'{\"a\": 1}\''

        d = {
            'i': 123,
            'j': 'abc',
            'k': True,
            'x': '<% $.abc %>',
            'y': '{{ _.abc }}',
            'z': {'a': 1}
        }

        self.assertDictEqual(params.parse_inline_params(s), d)
