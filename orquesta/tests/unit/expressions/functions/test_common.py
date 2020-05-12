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

from orquesta import exceptions as exc
from orquesta.expressions.functions import common as funcs


class CommonFunctionTest(unittest.TestCase):
    def test_convert_json_from_dict(self):
        self.assertDictEqual(funcs.json_({"k1": "v1"}), {"k1": "v1"})

    def test_convert_json_from_other_types(self):
        self.assertRaises(TypeError, funcs.json_, 123)
        self.assertRaises(TypeError, funcs.json_, False)
        self.assertRaises(TypeError, funcs.json_, ["a", "b"])
        self.assertRaises(TypeError, funcs.json_, object())

    def test_convert_json(self):
        self.assertDictEqual(funcs.json_('{"k1": "v1"}'), {"k1": "v1"})
        self.assertListEqual(funcs.json_('[{"a": 1}, {"b": 2}]'), [{"a": 1}, {"b": 2}])
        self.assertListEqual(funcs.json_("[5, 3, 5, 4]"), [5, 3, 5, 4])

    def test_zip_empty(self):
        self.assertListEqual(funcs.zip_(), [])

    def test_zip_null(self):
        self.assertListEqual(funcs.zip_(None), [])

    def test_zip_single(self):
        test_list = [1, 2, 3, 4, 5]

        self.assertListEqual(funcs.zip_(test_list), test_list)

    def test_zip_double(self):
        l1 = [1, 2, 3, 4, 5]
        l2 = ["a", "b", "c", "d", "e"]

        result = [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]

        self.assertListEqual(funcs.zip_(l1, l2), result)

    def test_zip_multiple(self):
        l1 = [1, 2, 3, 4, 5]
        l2 = ["a", "b", "c", "d", "e"]
        l3 = ["i", "ii", "iii", "iv", "v"]

        result = [(1, "a", "i"), (2, "b", "ii"), (3, "c", "iii"), (4, "d", "iv"), (5, "e", "v")]

        self.assertListEqual(funcs.zip_(l1, l2, l3), result)

    def test_zip_longest(self):
        l1 = [1, 2, 3, 4, 5]
        l2 = ["a", "b", "c"]

        result = [(1, "a"), (2, "b"), (3, "c"), (4, None), (5, None)]

        self.assertListEqual(funcs.zip_(l1, l2), result)

    def test_zip_longest_pad_empty(self):
        l1 = [1, 2, 3, 4, 5]
        l2 = ["a", "b", "c"]

        result = [(1, "a"), (2, "b"), (3, "c"), (4, "foobar"), (5, "foobar")]

        self.assertListEqual(funcs.zip_(l1, l2, pad="foobar"), result)

    def test_zip_longest_pad_value(self):
        l1 = []
        l2 = ["a", "b", "c"]
        value = "z"

        result = [("z", "a"), ("z", "b"), ("z", "c")]

        self.assertListEqual(funcs.zip_(l1, l2, pad=value), result)

    def test_ctx_get_by_key(self):
        data = {
            "__vars": {
                "a": 123,
                "b": "foobar",
                "c": True,
                "d": None,
                "e": {"foobar": "fubar"},
                "f": ["foobar", "fubar"],
            }
        }

        self.assertEqual(funcs.ctx_(data, "a"), 123)
        self.assertEqual(funcs.ctx_(data, "b"), "foobar")
        self.assertTrue(funcs.ctx_(data, "c"))
        self.assertIsNone(funcs.ctx_(data, "d"))
        self.assertDictEqual(funcs.ctx_(data, "e"), {"foobar": "fubar"})
        self.assertListEqual(funcs.ctx_(data, "f"), ["foobar", "fubar"])

    def test_ctx_get_all(self):
        data = {
            "__vars": {
                "a": 123,
                "b": "foobar",
                "c": True,
                "d": None,
                "e": {"foobar": "fubar"},
                "f": ["foobar", "fubar"],
                "__state": {"foobar": "fubar"},
            }
        }

        expected_data = {
            "a": 123,
            "b": "foobar",
            "c": True,
            "d": None,
            "e": {"foobar": "fubar"},
            "f": ["foobar", "fubar"],
        }

        self.assertDictEqual(funcs.ctx_(data), expected_data)

    def test_ctx_get_undefined(self):
        data = {"__vars": {}}

        self.assertRaises(exc.VariableUndefinedError, funcs.ctx_, data, "a")

    def test_ctx_get_private_var(self):
        data = {"__vars": {"__state": {"foobar": "fubar"}}}

        self.assertRaises(exc.VariableInaccessibleError, funcs.ctx_, data, "__state")
