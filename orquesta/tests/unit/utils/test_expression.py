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

from orquesta.utils import expression as expr_util


class ExpressionUtilsTest(unittest.TestCase):
    def test_format_error(self):
        expected = {"type": "yaql", "expression": "$.foo in $.bar", "message": "Unknown error."}

        actual = expr_util.format_error("yaql", "$.foo in $.bar", Exception("Unknown error."))

        self.assertEqual(expected, actual)

    def test_format_error_str(self):
        expected = {"type": "yaql", "expression": "$.foo in $.bar", "message": "Unknown error."}

        actual = expr_util.format_error("yaql", "$.foo in $.bar", "Unknown error.")

        self.assertEqual(expected, actual)

    def test_format_error_with_paths(self):
        expected = {
            "type": "yaql",
            "expression": "$.foo in $.bar",
            "spec_path": "path.to.error.in.spec",
            "schema_path": "path.to.reference.in.schema",
            "message": "Unknown error.",
        }

        actual = expr_util.format_error(
            "yaql",
            "$.foo in $.bar",
            Exception("Unknown error."),
            spec_path="path.to.error.in.spec",
            schema_path="path.to.reference.in.schema",
        )

        self.assertEqual(expected, actual)
