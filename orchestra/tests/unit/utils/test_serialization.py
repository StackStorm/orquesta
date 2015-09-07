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

import copy
import unittest2

from orchestra.utils import date
from orchestra.utils import jsonify


MOCK_DATETIME_STR = '2015-01-01T12:00:01.000000+01:00'

MOCK_JSON = {
    'k1': 'abc',
    'k2': 123,
    'k3': False,
    'k4': MOCK_DATETIME_STR,
    'k5': [1, 3, 5, 7, 9],
    'k6': {'a': 1, 'b': 2, 'c': 3}
}


class FakeModel(object):

    def __init__(self, *args, **kwargs):
        self.k1 = kwargs.get('k1', 'abc')
        self.k2 = kwargs.get('k2', 123)
        self.k3 = kwargs.get('k3', False)
        self.k4 = kwargs.get('k4', date.parse(MOCK_DATETIME_STR))
        self.k5 = kwargs.get('k5', [1, 3, 5, 7, 9])
        self.k6 = kwargs.get('k6', {'a': 1, 'b': 2, 'c': 3})


class SerializationTest(unittest2.TestCase):

    def test_serialize(self):
        doc = jsonify.serialize(FakeModel())
        self.assertDictEqual(MOCK_JSON, doc)

    def test_deserialize(self):
        obj = jsonify.deserialize(FakeModel, copy.deepcopy(MOCK_JSON))
        self.assertEqual(MOCK_JSON['k1'], obj.k1)
        self.assertEqual(MOCK_JSON['k2'], obj.k2)
        self.assertEqual(MOCK_JSON['k3'], obj.k3)
        self.assertEqual(date.parse(MOCK_JSON['k4']), obj.k4)
        self.assertListEqual(MOCK_JSON['k5'], obj.k5)
        self.assertDictEqual(MOCK_JSON['k6'], obj.k6)
