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

import unittest

from orquesta.utils import date as date_util
from orquesta.utils import jsonify as json_util


MOCK_DATETIME_STR = "2015-01-01T12:00:01.000000+01:00"

MOCK_JSON = {
    "k1": "abc",
    "k2": 123,
    "k3": False,
    "k4": MOCK_DATETIME_STR,
    "k5": [1, 3, 5, 7, 9],
    "k6": {"a": 1, "b": 2, "c": 3},
}

MOCK_JSON_UNSERIALIZEABLE = {"k1": object()}


class FakeModel(object):
    def __init__(self, *args, **kwargs):
        self.k1 = None
        self.k2 = None
        self.k3 = None
        self.k4 = None
        self.k5 = None
        self.k6 = None


class SerializationTest(unittest.TestCase):
    def test_serialize(self):
        obj = FakeModel()
        obj.k1 = MOCK_JSON["k1"]
        obj.k2 = MOCK_JSON["k2"]
        obj.k3 = MOCK_JSON["k3"]
        obj.k4 = MOCK_JSON["k4"]
        obj.k5 = MOCK_JSON["k5"]
        obj.k6 = MOCK_JSON["k6"]

        doc = json_util.serialize(obj)

        self.assertDictEqual(MOCK_JSON, doc)

    def test_serialize_unsupported_type(self):
        obj = FakeModel()
        obj.k1 = MOCK_JSON_UNSERIALIZEABLE["k1"]

        doc = json_util.serialize(obj)

        self.assertDictEqual(dict(), doc)

    def test_deserialize(self):
        obj = json_util.deserialize(FakeModel, MOCK_JSON)

        self.assertEqual(MOCK_JSON["k1"], obj.k1)
        self.assertEqual(MOCK_JSON["k2"], obj.k2)
        self.assertEqual(MOCK_JSON["k3"], obj.k3)
        self.assertEqual(date_util.parse(MOCK_JSON["k4"]), obj.k4)
        self.assertListEqual(MOCK_JSON["k5"], obj.k5)
        self.assertDictEqual(MOCK_JSON["k6"], obj.k6)

    def test_deserialize_unsupported_type(self):
        obj = json_util.deserialize(FakeModel, MOCK_JSON_UNSERIALIZEABLE)

        self.assertIsNone(obj.k1)
