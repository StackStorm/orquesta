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

from orquesta.utils import dictionary as dict_util
from orquesta.utils import jsonify as json_util


LEFT = {"k1": "123", "k2": "abc", "k3": {"k31": True, "k32": 1.0}}

RIGHT = {"k2": "def", "k3": {"k32": 2.0, "k33": {"k331": "foo"}}, "k4": "bar"}


class DictUtilsTest(unittest.TestCase):
    def test_dict_merge_overwrite(self):
        left = json_util.deepcopy(LEFT)
        right = json_util.deepcopy(RIGHT)

        dict_util.merge_dicts(left, right)

        expected = {
            "k1": "123",
            "k2": "def",
            "k3": {"k31": True, "k32": 2.0, "k33": {"k331": "foo"}},
            "k4": "bar",
        }

        self.assertDictEqual(left, expected)

    def test_dict_merge_overwrite_false(self):
        left = json_util.deepcopy(LEFT)
        right = json_util.deepcopy(RIGHT)

        dict_util.merge_dicts(left, right, overwrite=False)

        expected = {
            "k1": "123",
            "k2": "abc",
            "k3": {"k31": True, "k32": 1.0, "k33": {"k331": "foo"}},
            "k4": "bar",
        }

        self.assertDictEqual(left, expected)

    def test_dict_dot_notation_access(self):
        data = {
            "a": "foo",
            "b": {"c": "bar", "d": {"e": 123, "f": False, "g": {}, "h": None}},
            "x": {},
            "y": None,
        }

        self.assertEqual("foo", dict_util.get_dict_value(data, "a"))
        self.assertEqual("bar", dict_util.get_dict_value(data, "b.c"))
        self.assertEqual(123, dict_util.get_dict_value(data, "b.d.e"))
        self.assertFalse(dict_util.get_dict_value(data, "b.d.f"))
        self.assertDictEqual({}, dict_util.get_dict_value(data, "b.d.g"))
        self.assertIsNone(dict_util.get_dict_value(data, "b.d.h"))
        self.assertDictEqual({}, dict_util.get_dict_value(data, "x"))
        self.assertIsNone(dict_util.get_dict_value(data, "x.x"))
        self.assertIsNone(dict_util.get_dict_value(data, "y"))
        self.assertIsNone(dict_util.get_dict_value(data, "z"))

    def test_dict_dot_notation_access_type_error(self):
        data = {"a": "foo"}

        self.assertRaises(TypeError, dict_util.get_dict_value, data, "a.b")

    def test_dict_dot_notation_access_key_error(self):
        data = {"a": {}}

        self.assertRaises(KeyError, dict_util.get_dict_value, data, "a.b", raise_key_error=True)

    def test_dict_dot_notation_set_value(self):
        data = {
            "a": "foo",
            "b": {"c": "bar", "d": {"e": 123, "f": False, "g": {}, "h": None}},
            "x": {},
            "y": None,
        }

        # Test basic insert.
        dict_util.set_dict_value(data, "z", {"foo": "bar"})

        # Test insert via dot notation on existing node.
        dict_util.set_dict_value(data, "b.d.h", 2.0)

        # Test insert via dot notation on nonexistent nodes.
        dict_util.set_dict_value(data, "m.n.o", True)

        # Test insert non-null only.
        dict_util.set_dict_value(data, "b.d.i", None, insert_null=False)

        # Test insert null.
        dict_util.set_dict_value(data, "b.d.j", None, insert_null=True)

        expected_data = {
            "a": "foo",
            "b": {"c": "bar", "d": {"e": 123, "f": False, "g": {}, "h": 2.0, "j": None}},
            "m": {"n": {"o": True}},
            "x": {},
            "y": None,
            "z": {"foo": "bar"},
        }

        self.assertDictEqual(data, expected_data)

    def test_dict_dot_notation_set_value_type_error(self):
        data = {"a": "foo"}

        self.assertRaises(TypeError, dict_util.set_dict_value, data, "a.b", "foobar")

    def test_dict_dot_notation_set_value_key_error(self):
        data = {"a": {}}

        self.assertRaises(
            KeyError, dict_util.set_dict_value, data, "a.b", "foobar", raise_key_error=True
        )
