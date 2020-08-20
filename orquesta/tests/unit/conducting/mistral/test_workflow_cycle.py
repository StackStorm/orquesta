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
from orquesta.tests.unit.conducting.mistral import base


class CyclicWorkflowConductorTest(base.MistralWorkflowConductorTest, WorkflowComposerTest):
    def test_cycle(self):
        wf_name = "cycle"

        expected_task_seq = [
            "prep",
            "task1",
            "task2",
            "task3",
            "task1",
            "task2",
            "task3",
            "task1",
            "task2",
            "task3",
        ]

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(wf_spec, expected_task_seq,)
        # will throw
        mock.assert_conducting_sequences()

    def test_cycles(self):
        wf_name = "cycles"

        expected_task_seq = [
            "prep",
            "task1",
            "task2",
            "task3",
            "task4",
            "task2",
            "task5",
            "task1",
            "task2",
            "task3",
            "task4",
            "task2",
            "task5",
            "task1",
            "task2",
            "task3",
            "task4",
            "task2",
            "task5",
        ]

        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = WorkflowConductorMock(wf_spec, expected_task_seq,)
        # will throw
        mock.assert_conducting_sequences()
