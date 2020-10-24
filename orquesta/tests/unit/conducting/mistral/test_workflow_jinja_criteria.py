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

from orquesta.tests import mocks
from orquesta.tests.unit import base as test_base
from orquesta.tests.unit.conducting.mistral import base


class JinjaCriteriaConductorTest(base.MistralWorkflowConductorTest, test_base.WorkflowComposerTest):
    def test_decision_tree_with_jinja_expr(self):
        wf_name = "decision-jinja"

        # Test branch "a"
        expected_task_seq = ["t1", "a"]

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = mocks.WorkflowConductorMock(wf_spec, expected_task_seq, inputs={"which": "a"})
        # will throw
        mock.assert_conducting_sequences()

        # Test branch "b"
        expected_task_seq = ["t1", "b"]

        mock = mocks.WorkflowConductorMock(wf_spec, expected_task_seq, inputs={"which": "b"})
        # will throw
        mock.assert_conducting_sequences()

        # Test branch "c"
        expected_task_seq = ["t1", "c"]

        mock = mocks.WorkflowConductorMock(wf_spec, expected_task_seq, inputs={"which": "c"})
        # will throw
        mock.assert_conducting_sequences()
