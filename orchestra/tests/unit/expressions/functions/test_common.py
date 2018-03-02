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

from orchestra.expressions.functions import common as funcs


class CommonFunctionTest(unittest.TestCase):

    def test_convert_json_from_dict(self):
        self.assertDictEqual(funcs.json_(None, {'k1': 'v1'}), {'k1': 'v1'})

    def test_convert_json_from_other_types(self):
        self.assertRaises(TypeError, funcs.json_, None, 123)
        self.assertRaises(TypeError, funcs.json_, None, False)
        self.assertRaises(TypeError, funcs.json_, None, ['a', 'b'])
        self.assertRaises(TypeError, funcs.json_, None, object())

    def test_convert_json(self):
        self.assertDictEqual(funcs.json_(None, '{"k1": "v1"}'), {'k1': 'v1'})
        self.assertListEqual(funcs.json_(None, '[{"a": 1}, {"b": 2}]'), [{'a': 1}, {'b': 2}])
        self.assertListEqual(funcs.json_(None, '[5, 3, 5, 4]'), [5, 3, 5, 4])
