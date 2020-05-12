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
import yaml

from orquesta.tests.unit.specs import base as test_specs


class SpecTest(unittest.TestCase):
    def setUp(self):
        super(SpecTest, self).setUp()
        self.maxDiff = None

    def test_spec_init_arg_bad_yaml(self):
        self.assertRaises(ValueError, test_specs.MockSpec, "foobar")

    def test_spec_init_yaml(self):
        spec = """
        name: mock
        version: '1.0'
        description: This is a mock spec.
        inputs:
            - x
            - y: polo
        vars:
            var1: foobar
            var2: <% ctx().x %>
            var3: <% ctx().y %>
        attr1: foobar
        attr2:
            macro: polo
        attr3:
            - <% ctx().var1 %>
        attr5:
            attr1:
                attr1: <% ctx().var2 %> <% ctx().var3 %>
        """

        spec_obj = test_specs.MockSpec(spec)
        spec_dict = yaml.safe_load(spec)

        self.assertDictEqual(spec_obj.spec, spec_dict)
        self.assertEqual(spec_obj.name, spec_dict["name"])
        self.assertEqual(spec_obj.version, spec_dict["version"])
        self.assertEqual(spec_obj.description, spec_dict["description"])

        self.assertEqual(spec_obj.attr1, spec_dict["attr1"])
        self.assertDictEqual(spec_obj.attr2, spec_dict["attr2"])
        self.assertListEqual(spec_obj.attr3, spec_dict["attr3"])

        self.assertIsInstance(spec_obj.attr5, test_specs.MockJointSpec)
        self.assertIsInstance(spec_obj.attr5.attr1, test_specs.MockLeafSpec)

        self.assertEqual(spec_obj.attr5.attr1.attr1, spec_dict["attr5"]["attr1"]["attr1"])

        self.assertRaises(AttributeError, getattr, spec_obj, "attr9")

    def test_spec_valid_yaml(self):
        spec = """
        name: mock
        version: '1.0'
        description: This is a mock spec.
        inputs:
            - x
            - y: polo
        vars:
            var1: foobar
            var2: <% ctx().x %>
            var3: <% ctx().y %>
        attr1: foobar
        attr2:
            macro: polo
        attr3:
            - <% ctx().var1 %>
        attr5:
            attr1:
                attr1: <% ctx().var2 %> <% ctx().var3 %>
        """

        spec_obj = test_specs.MockSpec(spec)

        self.assertDictEqual(spec_obj.spec, yaml.safe_load(spec))
        self.assertDictEqual(spec_obj.inspect(), {})
