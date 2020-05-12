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

import copy
import unittest

from orquesta import conducting
from orquesta import statuses

MOCK_WORKFLOW_STATE = {
    "contexts": [],
    "routes": [],
    "sequence": [],
    "staged": [],
    "status": statuses.UNSET,
    "tasks": {},
}


class WorkflowStateTest(unittest.TestCase):
    def test_get_tasks(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task3", "route": 0},
            {"id": "task4", "route": 0},
            {"id": "task5", "route": 0},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)
        state = conducting.WorkflowState.deserialize(data)

        actual_task_sequence = state.get_tasks(last_occurrence=False)

        self.assertListEqual(actual_task_sequence, list(enumerate(task_sequence)))

    def test_get_tasks_by_task_id(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task2", "route": 1},
            {"id": "task2", "route": 2},
            {"id": "task3", "route": 0},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)
        state = conducting.WorkflowState.deserialize(data)

        expected_task_sequence = [
            (1, {"id": "task2", "route": 0}),
            (2, {"id": "task2", "route": 1}),
            (3, {"id": "task2", "route": 2}),
        ]

        actual_task_sequence = state.get_tasks(task_id="task2", last_occurrence=False)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

    def test_get_tasks_by_task_id_and_route(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task2", "route": 1},
            {"id": "task3", "route": 0},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)
        state = conducting.WorkflowState.deserialize(data)

        expected_task_sequence = [
            (1, {"id": "task2", "route": 0}),
            (2, {"id": "task2", "route": 0}),
        ]

        actual_task_sequence = state.get_tasks(task_id="task2", route=0, last_occurrence=False)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

        expected_task_sequence = [(3, {"id": "task2", "route": 1})]

        actual_task_sequence = state.get_tasks(task_id="task2", route=1, last_occurrence=False)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

    def test_get_tasks_by_status(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0, "status": "succeeded"},
            {"id": "task2", "route": 0, "status": "failed"},
            {"id": "task2", "route": 0, "status": "succeeded"},
            {"id": "task3", "route": 0, "status": "running"},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)
        state = conducting.WorkflowState.deserialize(data)

        expected_task_sequence = [
            (0, {"id": "task1", "route": 0, "status": "succeeded"}),
            (2, {"id": "task2", "route": 0, "status": "succeeded"}),
        ]

        actual_task_sequence = state.get_tasks_by_status(statuses.SUCCEEDED, last_occurrence=False)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

    def test_get_terminal_tasks(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0, "status": "succeeded"},
            {"id": "task2", "route": 0, "status": "succeeded", "term": True},
            {"id": "task3", "route": 0, "status": "succeeded", "term": True},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)
        state = conducting.WorkflowState.deserialize(data)

        expected_task_sequence = [
            (1, {"id": "task2", "route": 0, "status": "succeeded", "term": True}),
            (2, {"id": "task3", "route": 0, "status": "succeeded", "term": True}),
        ]

        actual_task_sequence = state.get_terminal_tasks()

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

    def test_get_tasks_last_occurrence(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0},
            {"id": "task1", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task3", "route": 0},
            {"id": "task3", "route": 0},
            {"id": "task3", "route": 0},
            {"id": "task4", "route": 0},
            {"id": "task5", "route": 0},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)

        task_map = {"task1__r0": 1, "task2__r0": 2, "task3__r0": 5, "task4__r0": 6, "task5__r0": 7}

        data["tasks"] = copy.deepcopy(task_map)

        state = conducting.WorkflowState.deserialize(data)

        expected_task_sequence = [
            (1, {"id": "task1", "route": 0}),
            (2, {"id": "task2", "route": 0}),
            (5, {"id": "task3", "route": 0}),
            (6, {"id": "task4", "route": 0}),
            (7, {"id": "task5", "route": 0}),
        ]

        actual_task_sequence = state.get_tasks(last_occurrence=True)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

    def test_get_tasks_last_occurrence_by_task_id(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task2", "route": 1},
            {"id": "task2", "route": 1},
            {"id": "task3", "route": 0},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)

        task_map = {"task1__r0": 0, "task2__r0": 2, "task2__r1": 4, "task3__r0": 5}

        data["tasks"] = copy.deepcopy(task_map)

        state = conducting.WorkflowState.deserialize(data)

        expected_task_sequence = [
            (2, {"id": "task2", "route": 0}),
            (4, {"id": "task2", "route": 1}),
        ]

        actual_task_sequence = state.get_tasks(task_id="task2", last_occurrence=True)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

    def test_get_tasks_last_occurrence_by_task_id_and_route(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task2", "route": 0},
            {"id": "task2", "route": 1},
            {"id": "task2", "route": 1},
            {"id": "task3", "route": 0},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)

        task_map = {"task1__r0": 0, "task2__r0": 2, "task2__r1": 4, "task3__r0": 5}

        data["tasks"] = copy.deepcopy(task_map)

        state = conducting.WorkflowState.deserialize(data)

        expected_task_sequence = [(2, {"id": "task2", "route": 0})]

        actual_task_sequence = state.get_tasks(task_id="task2", route=0, last_occurrence=True)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

        expected_task_sequence = [(4, {"id": "task2", "route": 1})]

        actual_task_sequence = state.get_tasks(task_id="task2", route=1, last_occurrence=True)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)

    def test_get_tasks_last_occurrence_by_status(self):
        data = copy.deepcopy(MOCK_WORKFLOW_STATE)

        task_sequence = [
            {"id": "task1", "route": 0, "status": "succeeded"},
            {"id": "task2", "route": 0, "status": "succeeded"},
            {"id": "task2", "route": 0, "status": "succeeded"},
            {"id": "task3", "route": 0, "status": "running"},
        ]

        data["sequence"] = copy.deepcopy(task_sequence)

        task_map = {"task1__r0": 0, "task2__r0": 2, "task3__r0": 3}

        data["tasks"] = copy.deepcopy(task_map)

        state = conducting.WorkflowState.deserialize(data)

        expected_task_sequence = [
            (0, {"id": "task1", "route": 0, "status": "succeeded"}),
            (2, {"id": "task2", "route": 0, "status": "succeeded"}),
        ]

        actual_task_sequence = state.get_tasks_by_status(statuses.SUCCEEDED, last_occurrence=True)

        self.assertListEqual(actual_task_sequence, expected_task_sequence)
