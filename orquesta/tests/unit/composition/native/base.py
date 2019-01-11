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

import json

from orquesta.tests.unit import base


class OrchestraWorkflowComposerTest(base.WorkflowComposerTest):

    @classmethod
    def setUpClass(cls):
        cls.spec_module_name = 'native'
        super(OrchestraWorkflowComposerTest, cls).setUpClass()

    def assert_wf_ex_routes(self, wf_name, expected_routes, debug=False):
        wf_ex_graph = self.compose_wf_ex_graph(wf_name)

        if debug:
            print(json.dumps(wf_ex_graph.get_routes(), indent=4))

        self.assertListEqual(wf_ex_graph.get_routes(), expected_routes)
