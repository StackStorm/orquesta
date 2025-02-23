# Copyright 2021 The StackStorm Authors.
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
from orquesta import statuses
from orquesta.tests.unit.conducting.native import base


class WithItemsWorkflowConductorTest(base.OrchestraWorkflowConductorTest):
    def test_basic_items_list(self):
        test_spec = {
            "workflow": self.get_wf_file_path("with-items-transition"),
            "expected_task_sequence": ["task1", "task2"],
            "mock_action_executions": [
                {"task_id": "task1", "result": "fee", "item_id": 0},
                {"task_id": "task1", "result": "fi", "item_id": 1},
                {"task_id": "task1", "result": "fo", "item_id": 2},
                {"task_id": "task1", "result": "fum", "item_id": 3},
            ],
            "expected_output": {"items": ["fee", "fi", "fo", "fum"]},
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_items_list_with_error(self):
        test_spec = {
            "workflow": self.get_wf_file_path("with-items-transition"),
            "expected_task_sequence": ["task1"],
            "mock_action_executions": [
                {"task_id": "task1", "result": "fee", "item_id": 0},
                {"task_id": "task1", "result": "fi", "item_id": 1},
                {"task_id": "task1", "result": "fo", "item_id": 2, "status": statuses.FAILED},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_items_list_with_error_and_remediation(self):
        test_spec = {
            "workflow": self.get_wf_file_path("with-items-remediate"),
            "expected_task_sequence": ["task1", "task2"],
            "mock_action_executions": [
                {"task_id": "task1", "result": "fee", "item_id": 0},
                {"task_id": "task1", "result": "fi", "item_id": 1},
                {"task_id": "task1", "result": None, "item_id": 2, "status": statuses.FAILED},
            ],
            "expected_output": {"items": ["fee", "fi", None]},
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_parallel_items_tasks(self):
        test_spec = {
            "workflow": self.get_wf_file_path("with-items-parallel"),
            "expected_task_sequence": ["task1", "task2", "task3"],
            "mock_action_executions": [
                {"task_id": "task1", "result": "fee", "item_id": 0},
                {"task_id": "task2", "result": "fie", "item_id": 0},
                {"task_id": "task1", "result": "fi", "item_id": 1},
                {"task_id": "task2", "result": "foh", "item_id": 1},
                {"task_id": "task1", "result": "fo", "item_id": 2},
                {"task_id": "task2", "result": "fum", "item_id": 2},
                {"task_id": "task1", "result": "fum", "item_id": 3},
            ],
            "expected_output": {
                "t1_items": ["fee", "fi", "fo", "fum"],
                "t2_items": ["fie", "foh", "fum"],
            },
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequence()

    def test_parallel_items_tasks_with_error(self):
        test_spec = {
            "workflow": self.get_wf_file_path("with-items-parallel"),
            "expected_task_sequence": ["task1", "task2"],
            "mock_action_executions": [
                {"task_id": "task1", "result": "fee", "item_id": 0},
                {"task_id": "task2", "result": "fie", "item_id": 0},
                {"task_id": "task1", "result": "fi", "item_id": 1},
                {"task_id": "task2", "result": "foh", "item_id": 1},
                {"task_id": "task1", "result": None, "item_id": 2, "status": statuses.FAILED},
                {"task_id": "task2", "result": "fum", "item_id": 2},
            ],
            "expected_workflow_status": statuses.FAILED,
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()
