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

from orchestra.composers import direct
from orchestra.specs.v2 import workflows as specs
from orchestra.utils import plugin
from orchestra.tests.unit import base


class DirectWorkflowComposerTest(base.DirectWorkflowComposerTest):

    def test_get_composer(self):
        self.assertEqual(
            plugin.get_module('orchestra.composers', self.composer_name),
            direct.DirectWorkflowComposer
        )

    def test_unsupported_spec_type(self):
        self.assertRaises(TypeError, self.composer.compose, {})

        wf_name = 'sequential'
        wf_def = self.get_wf_def(wf_name, rel_path='reverse')
        wf_spec = specs.ReverseWorkflowSpec(wf_name, wf_def[wf_name])

        self.assertRaises(TypeError, self.composer.compose, wf_spec)
