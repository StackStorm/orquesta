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

from orquesta.specs.mock.models import TestFileSpec


class TaskSpecTest(unittest.TestCase):
    def test_spec(self):
        mock_fixture_def = """
                version: 1.0
                description: A fixture
                file: "test.txt"
                expected_task_seq:
                    - "one"
                    - "two"
            """
        fixture_spec = TestFileSpec(mock_fixture_def, "mock")
        errors = fixture_spec.inspect()
        self.assertEqual(len(errors), 0)

    def test_missing_file(self):
        mock_fixture_def = """
                version: 1.0
                description: a fixture
                expected_task_seq:
                    - "one"
                    - "two"
            """
        fixture_spec = TestFileSpec(mock_fixture_def, "mock")
        errors = fixture_spec.inspect()
        self.assertEqual(len(errors), 1)

    def test_full_spec_valid(self):
        mock_fixture_def = """
                version: 1.0
                description: A basic fixture
                file: "test.txt"
                expected_task_seq:
                    - "one"
                    - "two"
                expected_routes:
                    -
                        - 0
                        - 1
                inputs:
                    myinput:
                        var1: 1
                        var2: 2

                mock_statuses:
                    - "succeeded"
                    - "succeeded"
                mock_results:
                    - false
                    - true
                expected_workflow_status: "succeeded"
                expected_output:
                    output:
                        parm1: false
                expected_term_tasks:
                    -
                        - one
                        - 1
                    -
                        - one
                        - 2
            """
        fixture_spec = TestFileSpec(mock_fixture_def, "mock")
        errors = fixture_spec.inspect()
        self.assertEqual(len(errors), 0)
