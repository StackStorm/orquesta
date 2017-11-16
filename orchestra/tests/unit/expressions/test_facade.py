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

from orchestra.expressions import base as expressions


class ExpressionEvaluatorTest(unittest.TestCase):

    def test_mix_types(self):
        expr = '<% $.foo %> and {{ _.foo }}'

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
