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

from orquesta.utils import parameters as args_util


class InlineParametersTest(unittest.TestCase):
    def test_parse_basic(self):
        tests = [
            ("x=null", {"x": None}),
            ("x=123", {"x": 123}),
            ("x=-123", {"x": -123}),
            ("x=0.123", {"x": 0.123}),
            ("x=-0.123", {"x": -0.123}),
            ("x=true", {"x": True}),
            ("x=false", {"x": False}),
            ('x="abc"', {"x": "abc"}),
            ("x='abc'", {"x": "abc"}),
        ]

        for s, d in tests:
            self.assertDictEqual(args_util.parse_inline_params(s)[0], d)

    def test_parse_dictionary(self):
        tests = [("x='{\"a\": 1}'", {"x": {"a": 1}})]

        for s, d in tests:
            self.assertDictEqual(args_util.parse_inline_params(s)[0], d)

    def test_parse_expression(self):
        tests = [("x=<% $.abc %>", {"x": "<% $.abc %>"}), ("x={{ _.abc }}", {"x": "{{ _.abc }}"})]

        for s, d in tests:
            self.assertDictEqual(args_util.parse_inline_params(s)[0], d)

    def test_parse_multiple(self):
        tests = [
            ('x="abc" y="def"', [{"x": "abc"}, {"y": "def"}]),
            ('y="def" x="abc"', [{"y": "def"}, {"x": "abc"}]),
            ('x="abc", y="def"', [{"x": "abc"}, {"y": "def"}]),
            ('y="def", x="abc"', [{"y": "def"}, {"x": "abc"}]),
            ('x="abc"; y="def"', [{"x": "abc"}, {"y": "def"}]),
            ('y="def"; x="abc"', [{"y": "def"}, {"x": "abc"}]),
        ]

        for s, d in tests:
            self.assertListEqual(args_util.parse_inline_params(s), d)

    def test_parse_combination(self):
        s = 'i=123 j="abc" k=true x=<% $.abc %> y={{ _.abc }} z=\'{"a": 1}\''

        d = [
            {"i": 123},
            {"j": "abc"},
            {"k": True},
            {"x": "<% $.abc %>"},
            {"y": "{{ _.abc }}"},
            {"z": {"a": 1}},
        ]

        self.assertListEqual(args_util.parse_inline_params(s), d)

    def test_parse_empty_string(self):
        self.assertListEqual(args_util.parse_inline_params(str()), [])

    def test_parse_null_type(self):
        self.assertListEqual(args_util.parse_inline_params(None), [])

    def test_parse_bool_input(self):
        tests = [
            ("x=true", [{"x": True}]),
            ("y=True", [{"y": True}]),
            ("c=TRUE", [{"c": True}]),
            ("x=false", [{"x": False}]),
            ("y=False", [{"y": False}]),
            ("c=FALSE", [{"c": False}]),
        ]

        for s, d in tests:
            self.assertListEqual(args_util.parse_inline_params(s), d)

    def test_parse_string_in_quotes_input(self):
        tests = [
            ('x="true"', [{"x": "true"}]),
            ('y="True"', [{"y": "True"}]),
            ('c="TRUE"', [{"c": "TRUE"}]),
            ('d="123"', [{"d": "123"}]),
            ('e="abcde"', [{"e": "abcde"}]),
            ('f=""', [{"f": ""}]),
            ("x='false'", [{"x": "false"}]),
            ("y='False'", [{"y": "False"}]),
            ("c='FALSE'", [{"c": "FALSE"}]),
            ("d='123'", [{"d": "123"}]),
            ("e='abcde'", [{"e": "abcde"}]),
            ("f=''", [{"f": ""}]),
        ]

        for s, d in tests:
            self.assertListEqual(args_util.parse_inline_params(s), d)

    def test_parse_other_types(self):
        self.assertListEqual(args_util.parse_inline_params(123), [])
        self.assertListEqual(args_util.parse_inline_params(True), [])
        self.assertListEqual(args_util.parse_inline_params([1, 2, 3]), [])
        self.assertListEqual(args_util.parse_inline_params({"a": 123}), [])
