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

from orquesta.tests.fixtures import loader as fixture_loader


class FixtureTest(unittest.TestCase):
    def test_fixture_loader(self):
        expected = {"version": 1.0, "basic": {"tasks": [{"task1": "foobar"}]}}

        content = fixture_loader.get_fixture_content("mock/basic.yaml", "workflows")

        self.assertIsInstance(content, dict)
        self.assertDictEqual(content, expected)

    def test_fixture_fixture_loader_raw(self):
        expected = "version: 1.0\nbasic:\n  tasks:\n    - task1: foobar\n"
        content = fixture_loader.get_fixture_content("mock/basic.yaml", "workflows", raw=True)

        self.assertIsInstance(content, str)
        self.assertEqual(content, expected)
