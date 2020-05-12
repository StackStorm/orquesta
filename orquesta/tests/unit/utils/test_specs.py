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

import yaml

from orquesta.specs import loader as spec_loader
from orquesta.tests.unit import base as test_base
from orquesta.utils import specs as spec_util


class SpecsUtilTest(test_base.WorkflowSpecTest):
    def setUp(self):
        super(SpecsUtilTest, self).setUp()
        self.spec_module = spec_loader.get_spec_module(self.spec_module_name)

    def test_convert_wf_def_dict_to_spec(self):
        wf_name = "basic"
        wf_def = self.get_wf_def(wf_name)

        self.assertIsInstance(wf_def, dict)

        wf_spec = spec_util.instantiate(self.spec_module_name, wf_def)

        self.assertIsInstance(wf_spec, self.spec_module.WorkflowSpec)
        self.assertEqual(wf_name, wf_spec.name)
        self.assertDictEqual(wf_def[wf_name], wf_spec.spec)

    def test_convert_wf_def_yaml_to_spec(self):
        wf_name = "basic"
        wf_def = self.get_wf_def(wf_name, raw=True)

        self.assertIsInstance(wf_def, str)

        wf_spec = spec_util.instantiate(self.spec_module_name, wf_def)

        self.assertIsInstance(wf_spec, self.spec_module.WorkflowSpec)
        self.assertEqual(wf_name, wf_spec.name)
        self.assertDictEqual(yaml.safe_load(wf_def)[wf_name], wf_spec.spec)

    def test_bad_wf_def_none(self):
        self.assertRaises(ValueError, spec_util.instantiate, self.spec_module_name, None)

    def test_bad_wf_def_empty(self):
        self.assertRaises(ValueError, spec_util.instantiate, self.spec_module_name, dict())

    def test_bad_wf_def_not_yaml(self):
        self.assertRaises(ValueError, spec_util.instantiate, self.spec_module_name, "foobar")

    def test_bad_wf_def_without_version(self):
        wf_name = "basic"
        wf_def = self.get_wf_def(wf_name)
        wf_def.pop("version")

        self.assertIsNone(wf_def.get("version"))

        self.assertRaises(ValueError, spec_util.instantiate, self.spec_module_name, wf_def)

    def test_bad_wf_def_unsupported_version(self):
        wf_name = "basic"
        wf_def = self.get_wf_def(wf_name)
        wf_def["version"] = 99.0

        self.assertRaises(ValueError, spec_util.instantiate, self.spec_module_name, wf_def)

    def test_deserialize(self):
        wf_name = "basic"
        wf_def = self.get_wf_def(wf_name)
        wf_spec_1 = spec_util.instantiate(self.spec_module_name, wf_def)
        wf_spec_2 = spec_util.deserialize(wf_spec_1.serialize())

        self.assertIsInstance(wf_spec_2, self.spec_module.WorkflowSpec)
        self.assertEqual(wf_name, wf_spec_2.name)
        self.assertDictEqual(wf_def[wf_name], wf_spec_2.spec)
