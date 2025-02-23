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


class SplitWorkflowConductorTest(base.OrchestraWorkflowConductorTest):
    def test_split(self):
        test_spec = {
            "workflow": self.get_wf_file_path("split"),
            "expected_routes": [
                [],  # default from start
                ["task2__t0"],  # task1 -> task2 -> task4
                ["task3__t0"],  # task1 -> task3 -> task4
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r0",
                "task4__r1",
                "task4__r2",
                "task5__r1",
                "task5__r2",
                "task6__r1",
                "task6__r2",
                "task7__r1",
                "task7__r2",
            ],
            "expected_term_tasks": ["task7__r1", "task7__r2"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_splits(self):
        test_spec = {
            "workflow": self.get_wf_file_path("splits"),
            "expected_routes": [
                [],  # default from start
                ["task1__t0"],  # task1 -> task8
                ["task2__t0"],  # task1 -> task2 -> task4
                ["task3__t0"],  # task1 -> task3 -> task4
                ["task2__t0", "task7__t0"],  # task1 -> task2 -> task4 -> task5/6 -> task7 -> task8
                ["task3__t0", "task7__t0"],  # task1 -> task3 -> task4 -> task5/6 -> task7 -> task8
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r0",
                "task8__r1",
                "task4__r2",
                "task4__r3",
                "task5__r2",
                "task5__r3",
                "task6__r2",
                "task6__r3",
                "task7__r2",
                "task7__r3",
                "task8__r4",
                "task8__r5",
            ],
            "expected_term_tasks": ["task8__r1", "task8__r4", "task8__r5"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_nested_splits(self):
        test_spec = {
            "workflow": self.get_wf_file_path("splits-nested"),
            "expected_routes": [
                [],  # default from start
                ["task2__t0"],  # task1 -> task2 -> task4
                ["task3__t0"],  # task1 -> task3 -> task4
                ["task2__t0", "task5__t0"],  # task1 -> task2 -> task4 -> task5 -> task7
                ["task3__t0", "task5__t0"],  # task1 -> task3 -> task4 -> task5 -> task7
                ["task2__t0", "task6__t0"],  # task1 -> task2 -> task4 -> task6 -> task7
                ["task3__t0", "task6__t0"],  # task1 -> task3 -> task4 -> task6 -> task7
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r0",
                "task4__r1",
                "task4__r2",
                "task5__r1",
                "task5__r2",
                "task6__r1",
                "task6__r2",
                "task7__r3",
                "task7__r4",
                "task7__r5",
                "task7__r6",
                "task8__r3",
                "task8__r4",
                "task8__r5",
                "task8__r6",
                "task9__r3",
                "task9__r4",
                "task9__r5",
                "task9__r6",
                "task10__r3",
                "task10__r4",
                "task10__r5",
                "task10__r6",
            ],
            "expected_term_tasks": ["task10__r3", "task10__r4", "task10__r5", "task10__r6"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_splits_mixed(self):
        test_spec = {
            "workflow": self.get_wf_file_path("splits-mixed"),
            "expected_routes": [
                [],
                ["task1__t0"],  # task1 -> task3
                ["task2__t0"],  # task2 -> task3
                ["task1__t0", "task3__t0"],  # task1 -> task3 -> task4
                ["task2__t0", "task3__t0"],  # task2 -> task3 -> task4
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r1",
                "task3__r2",
                "task4__r3",
                "task4__r4",
                "task5__r3",
                "task5__r4",
            ],
            "expected_term_tasks": ["task5__r3", "task5__r4"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_splits_mixed_alt_branch(self):
        test_spec = {
            "workflow": self.get_wf_file_path("splits-mixed"),
            "expected_routes": [
                [],
                ["task1__t0"],  # task1 -> task3
                ["task2__t0"],  # task2 -> task3
                ["task1__t0", "task3__t0"],  # task1 -> task3 -> task4
                ["task2__t0", "task3__t1"],  # task2 -> task3 -> task4
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r1",
                "task3__r2",
                "task4__r3",
                "task4__r4",
                "task5__r3",
                "task5__r4",
            ],
            "mock_action_executions": [
                {"task_id": "task3"},  # task3, 1
                {"task_id": "task3", "status": statuses.FAILED},  # task3, 2
            ],
            "expected_term_tasks": ["task5__r3", "task5__r4"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_splits_multiple_transition(self):
        test_spec = {
            "workflow": self.get_wf_file_path("splits-transition"),
            "expected_routes": [
                [],
                ["task1__t0"],  # task1 -> task3
                ["task2__t0"],  # task2 -> task3
                ["task1__t0", "task3__t0"],  # task1 -> task3 -> task4 (when #1)
                ["task1__t0", "task3__t1"],  # task1 -> task3 -> task4 (when #2)
                ["task2__t0", "task3__t0"],  # task2 -> task3 -> task4 (when #1)
                ["task2__t0", "task3__t1"],  # task2 -> task3 -> task4 (when #2)
            ],
            "expected_task_sequence": [
                "task1__r0",
                "task2__r0",
                "task3__r1",
                "task3__r2",
                "task4__r3",
                "task4__r4",
                "task4__r5",
                "task4__r6",
                "task5__r3",
                "task5__r4",
                "task5__r5",
                "task5__r6",
                "task6__r3",
                "task6__r4",
                "task6__r5",
                "task6__r6",
                "task7__r3",
                "task7__r4",
                "task7__r5",
                "task7__r6",
            ],
            "expected_term_tasks": ["task7__r3", "task7__r4", "task7__r5", "task7__r6"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_very_many_splits(self):
        test_spec = {
            "workflow": self.get_wf_file_path("splits-very-many"),
            "expected_routes": [
                [],
                ["init__t1"],
                ["init__t0"],
                ["init__t0", "task2__t0"],
                ["init__t0", "task2__t0", "task7__t0"],
                ["init__t0", "task2__t0", "task7__t0", "task10__t0"],
                ["init__t0", "task2__t0", "task7__t0", "task10__t0", "task13__t0"],
                ["init__t0", "task2__t0", "task7__t0", "task10__t0", "task13__t0", "task15__t0"],
                [
                    "init__t0",
                    "task2__t0",
                    "task7__t0",
                    "task10__t0",
                    "task13__t0",
                    "task15__t0",
                    "task17__t0",
                ],
            ],
            "expected_task_sequence": [
                "init__r0",
                "notify__r1",
                "task2__r2",
                "task5__r3",
                "task7__r3",
                "task9__r4",
                "task10__r4",
                "task11__r5",
                "task12__r5",
                "task13__r5",
                "task14__r6",
                "task15__r6",
                "task17__r7",
                "notify__r8",
            ],
            "expected_term_tasks": ["notify__r1", "notify__r8"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()

    def test_very_many_splits_alt(self):
        test_spec = {
            "workflow": self.get_wf_file_path("splits-very-many"),
            "inputs": {
                "fork1": True,
                "fork2": True,
                "fork3": True,
                "fork4": True,
                "fork5": True,
                "fork6": True,
                "fork7": True,
                "fork8": True,
            },
            "mock_action_executions": [
                {"task_id": "task16", "result": False},
                {"task_id": "task16", "result": True},
                {"task_id": "task18", "result": False},
                {"task_id": "task18", "result": True},
            ],
            "expected_routes": [
                [],
                ["init__t0"],
                ["task1__t0"],
                ["task1__t0", "task4__t0"],
                ["task1__t0", "task4__t0", "task6__t0"],
                ["task1__t0", "task4__t0", "task6__t0", "task8__t0"],
                ["task1__t0", "task4__t0", "task6__t0", "task8__t0", "task9__t0"],
                ["task1__t0", "task4__t0", "task6__t0", "task8__t0", "task9__t0", "task13__t0"],
                [
                    "task1__t0",
                    "task4__t0",
                    "task6__t0",
                    "task8__t0",
                    "task9__t0",
                    "task13__t0",
                    "task16__t0",
                ],
                [
                    "task1__t0",
                    "task4__t0",
                    "task6__t0",
                    "task8__t0",
                    "task9__t0",
                    "task13__t0",
                    "task16__t0",
                    "task19__t0",
                ],
            ],
            "expected_task_sequence": [
                "init__r0",
                "notify__r1",
                "task1__r0",
                "task2__r2",
                "task3__r2",
                "task4__r2",
                "task5__r3",
                "task6__r3",
                "task8__r4",
                "task9__r5",
                "task11__r6",
                "task12__r6",
                "task13__r6",
                "task14__r7",
                "task16__r7",
                "task16__r7",
                "task17__r8",
                "task18__r8",
                "task18__r8",
                "task19__r8",
                "notify__r9",
            ],
            "expected_term_tasks": ["notify__r1", "notify__r9"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)
        rehearsal.assert_conducting_sequence()
