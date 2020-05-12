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
from orquesta.utils import schema as schema_util


class SchemaUtilsTest(unittest.TestCase):
    def test_get_schema_type(self):
        self.assertEqual("object", schema_util.get_schema_type({"type": "object"}))
        self.assertIsNone(schema_util.get_schema_type({}))
        self.assertIsNone(schema_util.get_schema_type(None))

    def test_check_schema_mergeable(self):
        schema_util.check_schema_mergeable(None)
        schema_util.check_schema_mergeable({})
        schema_util.check_schema_mergeable({"type": "object"})
        schema_util.check_schema_mergeable({"type": "array"})

    def test_check_schema_unmergeable(self):
        self.assertRaises(
            exc.SchemaIncompatibleError, schema_util.check_schema_mergeable, {"type": None}
        )

        self.assertRaises(
            exc.SchemaIncompatibleError, schema_util.check_schema_mergeable, {"type": "string"}
        )

    def test_check_schemas_compatible(self):
        cases = [{"type": "object"}, {"type": "array"}, {"type": None}, {}, None]

        for s in cases:
            self.assertEqual(
                schema_util.get_schema_type(s), schema_util.check_schemas_compatible(s, s)
            )

    def test_check_schemas_incompatible(self):
        self.assertRaises(
            exc.SchemaIncompatibleError,
            schema_util.check_schemas_compatible,
            {"type": "object"},
            {"type": "array"},
        )

        self.assertRaises(
            exc.SchemaIncompatibleError,
            schema_util.check_schemas_compatible,
            {"type": "object"},
            None,
        )

        self.assertRaises(
            exc.SchemaIncompatibleError,
            schema_util.check_schemas_compatible,
            {"type": "object"},
            {},
        )

    def test_merge_schema_noop(self):
        s = {
            "type": "object",
            "properties": {"name": {"type": "string"}, "description": {"type": "string"}},
        }

        self.assertDictEqual(s, schema_util.merge_schema(s, None))
        self.assertDictEqual(s, schema_util.merge_schema(None, s))
        self.assertDictEqual({}, schema_util.merge_schema(None, None))
        self.assertDictEqual({}, schema_util.merge_schema({}, {}))

    def test_merge_schema_blank(self):
        s = {
            "type": "object",
            "properties": {"name": {"type": "string"}, "description": {"type": "string"}},
        }

        s_blank_obj = {"type": "object"}
        s_blank_arr = {"type": "array"}

        self.assertDictEqual(s, schema_util.merge_schema(s, s_blank_obj))
        self.assertDictEqual(s, schema_util.merge_schema(s_blank_obj, s))
        self.assertDictEqual(s, schema_util.merge_schema(s, s_blank_arr))
        self.assertDictEqual(s, schema_util.merge_schema(s_blank_arr, s))
        self.assertDictEqual({}, schema_util.merge_schema(s_blank_obj, s_blank_obj))
        self.assertDictEqual({}, schema_util.merge_schema(s_blank_arr, s_blank_arr))
        self.assertDictEqual({}, schema_util.merge_schema(s_blank_obj, s_blank_arr))
        self.assertDictEqual({}, schema_util.merge_schema(s_blank_arr, s_blank_obj))
        self.assertDictEqual({}, schema_util.merge_schema(None, s_blank_obj))
        self.assertDictEqual({}, schema_util.merge_schema(s_blank_obj, None))
        self.assertDictEqual({}, schema_util.merge_schema(None, s_blank_arr))
        self.assertDictEqual({}, schema_util.merge_schema(s_blank_arr, None))
        self.assertDictEqual({}, schema_util.merge_schema({}, s_blank_obj))
        self.assertDictEqual({}, schema_util.merge_schema(s_blank_obj, {}))
        self.assertDictEqual({}, schema_util.merge_schema({}, s_blank_arr))
        self.assertDictEqual({}, schema_util.merge_schema(s_blank_arr, {}))

    def test_merge_object_schema(self):
        s1 = {
            "type": "object",
            "properties": {"name": {"type": "string"}, "counter": {"type": "integer"}},
            "required": ["name"],
            "additionalProperties": False,
        }

        s2 = {
            "type": "object",
            "properties": {"description": {"type": "string"}, "counter": {"type": "number"}},
            "required": ["counter"],
            "additionalProperties": True,
        }

        expected = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "description": {"type": "string"},
                "counter": {"type": "number"},
            },
            "required": ["counter", "name"],
            "additionalProperties": False,
        }

        self.assertDictEqual(expected, schema_util.merge_schema(s1, s2))

    def test_merge_object_schema_overwrite_false(self):
        s1 = {
            "type": "object",
            "properties": {"name": {"type": "string"}, "counter": {"type": "integer"}},
            "required": ["name"],
            "additionalProperties": False,
        }

        s2 = {
            "type": "object",
            "properties": {"description": {"type": "string"}, "counter": {"type": "number"}},
            "required": ["counter"],
            "additionalProperties": True,
        }

        expected = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "description": {"type": "string"},
                "counter": {"type": "integer"},
            },
            "required": ["counter", "name"],
            "additionalProperties": False,
        }

        self.assertDictEqual(expected, schema_util.merge_schema(s1, s2, False))

    def test_merge_object_schema_patterned_properties(self):
        s1 = {
            "type": "object",
            "patternProperties": {"^version$": {"type": "integer"}},
            "minProperties": 0,
            "maxProperties": 10,
        }

        s2 = {
            "type": "object",
            "patternProperties": {
                r"^version$": {"type": "number"},
                r"^(?!version)\w+$": {"type": "string"},
            },
            "minProperties": 1,
            "maxProperties": 100,
        }

        expected = {
            "type": "object",
            "patternProperties": {
                r"^version$": {"type": "number"},
                r"^(?!version)\w+$": {"type": "string"},
            },
            "minProperties": 1,
            "maxProperties": 10,
        }

        self.assertDictEqual(expected, schema_util.merge_schema(s1, s2))

    def test_merge_object_schema_patterned_properties_overwrite_false(self):
        s1 = {
            "type": "object",
            "patternProperties": {"^version$": {"type": "integer"}},
            "minProperties": 1,
            "maxProperties": 100,
        }

        s2 = {
            "type": "object",
            "patternProperties": {
                r"^version$": {"type": "number"},
                r"^(?!version)\w+$": {"type": "string"},
            },
            "minProperties": 0,
            "maxProperties": 10,
        }

        expected = {
            "type": "object",
            "patternProperties": {
                r"^version$": {"type": "integer"},
                r"^(?!version)\w+$": {"type": "string"},
            },
            "minProperties": 1,
            "maxProperties": 100,
        }

        self.assertDictEqual(expected, schema_util.merge_schema(s1, s2, False))

    def test_merge_array_schema(self):
        s1 = {
            "type": "array",
            "items": [{"type": "string"}],
            "uniqueItems": True,
            "minItems": 1,
            "maxItems": 10,
        }

        s2 = {
            "type": "array",
            "items": [{"type": "number"}],
            "uniqueItems": True,
            "minItems": 10,
            "maxItems": 100,
        }

        expected = {
            "type": "array",
            "items": [{"type": "number"}],
            "uniqueItems": True,
            "minItems": 10,
            "maxItems": 100,
        }

        self.assertDictEqual(expected, schema_util.merge_schema(s1, s2))

    def test_merge_array_schema_overwrite_false(self):
        s1 = {
            "type": "array",
            "items": [{"type": "string"}],
            "uniqueItems": True,
            "minItems": 1,
            "maxItems": 10,
        }

        s2 = {
            "type": "array",
            "items": [{"type": "number"}],
            "uniqueItems": True,
            "minItems": 10,
            "maxItems": 100,
        }

        expected = {
            "type": "array",
            "items": [{"type": "string"}],
            "uniqueItems": True,
            "minItems": 1,
            "maxItems": 10,
        }

        self.assertDictEqual(expected, schema_util.merge_schema(s1, s2, False))

    def test_merge_array_schema_minimal(self):
        s1 = {"type": "array", "items": [{"type": "string"}]}

        s2 = {"type": "array", "items": [{"type": "number"}]}

        expected = {"type": "array", "items": [{"type": "number"}]}

        self.assertDictEqual(expected, schema_util.merge_schema(s1, s2))

    def test_merge_array_schema_skip_default(self):
        s1 = {
            "type": "array",
            "items": [{"type": "string"}],
            "uniqueItems": True,
            "minItems": 1,
            "maxItems": 10,
        }

        s2 = {
            "type": "array",
            "items": [{"type": "number"}],
            "uniqueItems": False,
            "minItems": 0,
            "maxItems": 0,
        }

        expected = {"type": "array", "items": [{"type": "number"}]}

        self.assertDictEqual(expected, schema_util.merge_schema(s1, s2))
