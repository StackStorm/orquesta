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

from orchestra import specs
from orchestra.specs import utils
from orchestra.tests.unit import base


class SpecUtilsTest(base.WorkflowSpecTest):

    def test_convert_direct_wf_def_dict_to_spec(self):
        wf_name = 'sequential'
        wf_def = self.get_wf_def(wf_name, rel_path='direct')

        self.assertIsInstance(wf_def, dict)

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        self.assertIsInstance(wf_spec, specs.DirectWorkflowSpec)
        self.assertDictEqual(wf_def, wf_spec.definition)

    def test_convert_direct_wf_def_yaml_to_spec(self):
        wf_name = 'sequential'
        wf_def = self.get_wf_def(wf_name, rel_path='direct', raw=True)

        self.assertIsInstance(wf_def, str)

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        self.assertIsInstance(wf_spec, specs.DirectWorkflowSpec)
        self.assertDictEqual(yaml.safe_load(wf_def), wf_spec.definition)

    def test_convert_reverse_wf_def_dict_to_spec(self):
        wf_name = 'sequential'
        wf_def = self.get_wf_def(wf_name, rel_path='reverse')

        self.assertIsInstance(wf_def, dict)

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        self.assertIsInstance(wf_spec, specs.ReverseWorkflowSpec)
        self.assertDictEqual(wf_def, wf_spec.definition)

    def test_convert_reverse_wf_def_yaml_to_spec(self):
        wf_name = 'sequential'
        wf_def = self.get_wf_def(wf_name, rel_path='reverse', raw=True)

        self.assertIsInstance(wf_def, str)

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        self.assertIsInstance(wf_spec, specs.ReverseWorkflowSpec)
        self.assertDictEqual(yaml.safe_load(wf_def), wf_spec.definition)
