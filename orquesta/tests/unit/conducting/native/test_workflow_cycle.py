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


class CyclicWorkflowConductorTest(base.OrchestraWorkflowConductorTest):
    def test_cycle(self):
        test_spec = {
            "workflow": self.get_wf_file_path("cycle"),
            "expected_task_sequence": [
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
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_cycles(self):
        test_spec = {
            "workflow": self.get_wf_file_path("cycles"),
            "expected_task_sequence": [
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
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_cycle_retry(self):
        test_spec = {
            "workflow": self.get_wf_file_path("cycle-retry"),
            "expected_task_sequence": [
                "init",
                "task1",
                "task1",
                "task1",
                "task2",
            ],
            "mock_action_executions": [
                {"task_id": "task1", "num_iter": 2, "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_rollback_retry(self):
        test_spec = {
            "workflow": self.get_wf_file_path("rollback-retry"),
            "expected_task_sequence": [
                "init",
                "check",
                "create",
                "rollback",
                "check",
                "delete",
            ],
            "mock_action_executions": [
                {"task_id": "check", "status": statuses.FAILED},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_cycle_and_fork(self):
        test_spec = {
            "workflow": self.get_wf_file_path("cycle-fork"),
            "expected_task_sequence": [
                "init",
                "query",
                "decide_cheer",
                "decide_work",
                "cheer",
                "notify_work",
                "toil",
                "query",
                "decide_cheer",
                "decide_work",
            ],
            "mock_action_executions": [
                {"task_id": "query", "seq_id": 1, "result": True},
                {"task_id": "query", "seq_id": 7, "result": False},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()
