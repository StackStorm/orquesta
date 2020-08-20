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

from orquesta.tests.mocks import WorkflowConductorMock
from orquesta.tests.unit.base import WorkflowComposerTest
from orquesta.tests.unit.conducting.native import base


class BasicWorkflowConductorTest(base.OrchestraWorkflowConductorTest, WorkflowComposerTest):
    def test_sequential(self):
        wf_name = "sequential"

        expected_task_seq = ["task1", "task2", "task3", "continue"]

        mock_results = [
            "Stanley",
            "All your base are belong to us!",
            "Stanley, All your base are belong to us!",
        ]

        expected_output = {"greeting": mock_results[2]}

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            inputs={"name": "Stanley"},
            mock_results=mock_results,
            expected_output=expected_output,
        )
        # will throw
        mock.assert_conducting_sequences()

    def test_parallel(self):
        wf_name = "parallel"

        expected_task_seq = ["task1", "task4", "task2", "task5", "task3", "task6"]

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(wf_spec, expected_task_seq,)

        mock.assert_conducting_sequences()

    def test_branching(self):
        wf_name = "branching"

        expected_task_seq = ["task1", "task2", "task4", "task3", "task5"]

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(wf_spec, expected_task_seq,)

        mock.assert_conducting_sequences()

    def test_decision_tree(self):
        wf_name = "decision"

        # Test branch "a"
        expected_task_seq = ["t1", "a"]
        inputs = {"which": "a"}
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(wf_spec, expected_task_seq, inputs=inputs)
        mock.assert_conducting_sequences()

        # Test branch "b"
        expected_task_seq = ["t1", "b"]
        inputs = {"which": "b"}
        mock = WorkflowConductorMock(wf_spec, expected_task_seq, inputs=inputs)
        mock.assert_conducting_sequences()

        # Test branch "c"
        expected_task_seq = ["t1", "c"]
        inputs = {"which": "c"}
        mock = WorkflowConductorMock(wf_spec, expected_task_seq, inputs=inputs)
        mock.assert_conducting_sequences()
