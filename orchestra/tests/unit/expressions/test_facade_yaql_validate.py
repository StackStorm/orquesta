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

from orchestra.tests.unit import base


class YAQLFacadeValidationTest(base.ExpressionFacadeEvaluatorTest):

    def test_basic_validate(self):
        self.assertListEqual([], self.validate('<% 1 %>'))
        self.assertListEqual([], self.validate('<% abc %>'))
        self.assertListEqual([], self.validate('<% 1 + 2 %>'))
        self.assertListEqual([], self.validate('<% $.foo %>'))
        self.assertListEqual([], self.validate('<% $.a1 + $.a2 %>'))

    def test_parse_error(self):
        expr = '<% <% $.foo %> %>'
        errors = self.validate(expr)

        self.assertEqual(1, len(errors))
        self.assertIn('Parse error', errors[0]['message'])

    def test_lexical_error(self):
        expr = '<% {"a": 123} %>'
        errors = self.validate(expr)

        self.assertEqual(1, len(errors))
        self.assertIn('Lexical error', errors[0]['message'])

    def test_multiple_errors(self):
        expr = '<% 1 +/ 2 %> and <% {"a": 123} %>'
        errors = self.validate(expr)

        self.assertEqual(2, len(errors))
        self.assertIn('Parse error', errors[0]['message'])
        self.assertIn('Lexical error', errors[1]['message'])
