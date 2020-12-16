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

from orquesta import rehearsing
from orquesta.tests.unit.conducting.native import base


class BasicWorkflowConductorTest(base.OrchestraWorkflowConductorTest):
    def test_sequential(self):
        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "inputs": {"name": "Stanley"},
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [
                {"task_id": "task1", "result": "Stanley"},
                {"task_id": "task2", "result": "All your base are belong to us!"},
                {"task_id": "task3", "result": "Stanley, All your base are belong to us!"},
            ],
            "expected_output": {"greeting": "Stanley, All your base are belong to us!"},
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_parallel(self):
        test_spec = {
            "workflow": self.get_wf_file_path("parallel"),
            "expected_task_sequence": ["task1", "task4", "task2", "task5", "task3", "task6"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_branching(self):
        test_spec = {
            "workflow": self.get_wf_file_path("branching"),
            "expected_task_sequence": ["task1", "task2", "task4", "task3", "task5"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_decision_tree(self):
        # Test branch "a"
        test_spec = {
            "workflow": self.get_wf_file_path("decision"),
            "inputs": {"which": "a"},
            "expected_task_sequence": ["t1", "a"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

        # Test branch "b"
        test_spec = {
            "workflow": self.get_wf_file_path("decision"),
            "inputs": {"which": "b"},
            "expected_task_sequence": ["t1", "b"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

        # Test branch "c"
        test_spec = {
            "workflow": self.get_wf_file_path("decision"),
            "inputs": {"which": "c"},
            "expected_task_sequence": ["t1", "c"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()
