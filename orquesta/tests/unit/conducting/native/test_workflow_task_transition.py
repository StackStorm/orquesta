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


class TaskTransitionWorkflowConductorTest(base.OrchestraWorkflowConductorTest):
    def test_no_error(self):
        test_spec = {
            "workflow": self.get_wf_file_path("error-handling"),
            "expected_task_sequence": ["task1", "task2"],
            "expected_term_tasks": ["task2"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_on_error(self):
        test_spec = {
            "workflow": self.get_wf_file_path("error-handling"),
            "expected_task_sequence": ["task1", "task3"],
            "mock_action_executions": [{"task_id": "task1", "status": statuses.FAILED}],
            "expected_term_tasks": ["task3"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_on_complete_from_succeeded_branch(self):
        test_spec = {
            "workflow": self.get_wf_file_path("task-on-complete"),
            "expected_task_sequence": ["task1", "task2", "task4"],
            "expected_term_tasks": ["task2", "task4"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_on_complete_from_failed_branch(self):
        test_spec = {
            "workflow": self.get_wf_file_path("task-on-complete"),
            "expected_task_sequence": ["task1", "task3", "task4"],
            "mock_action_executions": [{"task_id": "task1", "status": statuses.FAILED}],
            "expected_term_tasks": ["task3", "task4"],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_task_transitions_split_from_succeeded_branch(self):
        test_spec = {
            "workflow": self.get_wf_file_path("task-transitions-split"),
            "expected_routes": [
                [],  # default from start
                ["task1__t0"],  # task1 -> task2 (when #1)
                ["task1__t2"],  # task1 -> task2 (when #3)
            ],
            "expected_task_sequence": [("task1", 0), ("task2", 1), ("task2", 2)],
            "expected_term_tasks": [("task2", 1), ("task2", 2)],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_task_transitions_split_from_failed_branch(self):
        test_spec = {
            "workflow": self.get_wf_file_path("task-transitions-split"),
            "expected_routes": [
                [],  # default from start
                ["task1__t0"],  # task1 -> task2 (when #1)
                ["task1__t1"],  # task1 -> task2 (when #2)
            ],
            "expected_task_sequence": [("task1", 0), ("task2", 1), ("task2", 2)],
            "mock_action_executions": [{"task_id": "task1", "status": statuses.FAILED}],
            "expected_term_tasks": [("task2", 1), ("task2", 2)],
        }

        test = rehearsing.WorkflowTestCase(test_spec)
        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()
