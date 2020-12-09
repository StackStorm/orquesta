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
from orquesta import requests
from orquesta import statuses
from orquesta.tests.unit.conducting.native import base


class WorkflowConductorRerunTest(base.OrchestraWorkflowConductorTest):
    def test_fail_single_branch(self):
        wf_name = "parallel"

        # Fail task2.
        expected_task_seq = ["task1", "task4", "task2", "task5"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task2", status=statuses.FAILED),
        ]

        expected_term_tasks = ["task2", "task5"]

        test1 = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal1 = rehearsing.WorkflowRehearsal(test1)
        rehearsal1.assert_conducting_sequences()

        # Rerun entire workflow and fail task2 again.
        expected_task_seq = ["task1", "task4", "task2", "task5", "task2", "task6"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task2", status=statuses.FAILED),
        ]

        expected_term_tasks = ["task2", "task6"]

        test2 = rehearsing.WorkflowRerunTestCase(
            rehearsal1.conductor,
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal2 = rehearsing.WorkflowRehearsal(test2)
        rehearsal2.assert_conducting_sequences()

        # Rerun workflow from task2 only and complete the workflow.
        expected_task_seq = ["task1", "task4", "task2", "task5", "task2", "task6", "task2", "task3"]

        expected_term_tasks = ["task3", "task6"]

        test3 = rehearsing.WorkflowRerunTestCase(
            rehearsal2.conductor,
            expected_task_seq,
            rerun_tasks=[requests.TaskRerunRequest("task2", 0)],
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal3 = rehearsing.WorkflowRehearsal(test3)
        rehearsal3.assert_conducting_sequences()

    def test_fail_multiple_branches(self):
        wf_name = "parallel"

        # Fail task2 and task5.
        expected_task_seq = ["task1", "task4", "task2", "task5"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task2", status=statuses.FAILED),
            rehearsing.MockActionExecution("task5", status=statuses.FAILED),
        ]

        expected_term_tasks = ["task2", "task5"]

        test1 = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal1 = rehearsing.WorkflowRehearsal(test1)
        rehearsal1.assert_conducting_sequences()

        # Rerun and complete workflow.
        expected_task_seq = ["task1", "task4", "task2", "task5", "task2", "task5", "task3", "task6"]

        expected_term_tasks = ["task3", "task6"]

        test2 = rehearsing.WorkflowRerunTestCase(
            rehearsal1.conductor,
            expected_task_seq,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal2 = rehearsing.WorkflowRehearsal(test2)
        rehearsal2.assert_conducting_sequences()

    def test_fail_single_before_join(self):
        wf_name = "join"

        # Fail task3 before join at task6.
        expected_task_seq = ["task1", "task2", "task4", "task3", "task5"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task3", status=statuses.FAILED),
        ]

        expected_term_tasks = ["task3", "task5"]

        test1 = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal1 = rehearsing.WorkflowRehearsal(test1)
        rehearsal1.assert_conducting_sequences()

        # Rerun and complete workflow.
        expected_task_seq = ["task1", "task2", "task4", "task3", "task5", "task3", "task6", "task7"]

        expected_term_tasks = ["task7"]

        test2 = rehearsing.WorkflowRerunTestCase(
            rehearsal1.conductor,
            expected_task_seq,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal2 = rehearsing.WorkflowRehearsal(test2)
        rehearsal2.assert_conducting_sequences()

    def test_fail_multiple_before_join(self):
        wf_name = "join"

        # Fail task3 before join at task6.
        expected_task_seq = ["task1", "task2", "task4", "task3", "task5"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task3", status=statuses.FAILED),
            rehearsing.MockActionExecution("task5", status=statuses.FAILED),
        ]

        expected_term_tasks = ["task3", "task5"]

        test1 = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal1 = rehearsing.WorkflowRehearsal(test1)
        rehearsal1.assert_conducting_sequences()

        # Rerun and complete workflow.
        expected_task_seq = [
            "task1",
            "task2",
            "task4",
            "task3",
            "task5",
            "task3",
            "task5",
            "task6",
            "task7",
        ]

        expected_term_tasks = ["task7"]

        test2 = rehearsing.WorkflowRerunTestCase(
            rehearsal1.conductor,
            expected_task_seq,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal2 = rehearsing.WorkflowRehearsal(test2)
        rehearsal2.assert_conducting_sequences()

    def test_fail_at_join(self):
        wf_name = "join"

        self.assert_spec_inspection(wf_name)

        # Fail task3 before join at task6.
        expected_task_seq = ["task1", "task2", "task4", "task3", "task5", "task6"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task6", status=statuses.FAILED),
        ]

        expected_term_tasks = ["task6"]

        test1 = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal1 = rehearsing.WorkflowRehearsal(test1)
        rehearsal1.assert_conducting_sequences()

        # Rerun and complete workflow.
        expected_task_seq = ["task1", "task2", "task4", "task3", "task5", "task6", "task6", "task7"]

        expected_term_tasks = ["task7"]

        test2 = rehearsing.WorkflowRerunTestCase(
            rehearsal1.conductor,
            expected_task_seq,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal2 = rehearsing.WorkflowRehearsal(test2)
        rehearsal2.assert_conducting_sequences()

    def test_fail_cycle(self):
        wf_name = "cycle"

        # Fail task3 which is part of the cycle task1->task2->task3->task1.
        expected_task_seq = ["prep", "task1", "task2", "task3", "task1", "task2", "task3"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task3", seq_id=6, status=statuses.FAILED),
        ]

        expected_term_tasks = ["task3"]

        test1 = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal1 = rehearsing.WorkflowRehearsal(test1)
        rehearsal1.assert_conducting_sequences()

        # Rerun and complete workflow.
        expected_task_seq = [
            "prep",
            "task1",
            "task2",
            "task3",
            "task1",
            "task2",
            "task3",
            "task3",
            "task1",
            "task2",
            "task3",
        ]

        expected_term_tasks = ["task3"]

        test2 = rehearsing.WorkflowRerunTestCase(
            rehearsal1.conductor,
            expected_task_seq,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal2 = rehearsing.WorkflowRehearsal(test2)
        rehearsal2.assert_conducting_sequences()

    def test_fail_at_single_split(self):
        wf_name = "split"

        # Fail task5 at one of the split/fork.
        expected_routes = [
            [],  # default from start
            ["task2__t0"],  # task1 -> task2 -> task4
            ["task3__t0"],  # task1 -> task3 -> task4
        ]

        expected_task_seq = [
            ("task1", 0),
            ("task2", 0),
            ("task3", 0),
            ("task4", 1),
            ("task4", 2),
            ("task5", 1),
            ("task5", 2),
            ("task6", 1),
            ("task6", 2),
        ]

        mock_action_executions = [
            rehearsing.MockActionExecution("task5", seq_id=6, status=statuses.FAILED),
        ]

        expected_term_tasks = [("task5", 2), ("task6", 1), ("task6", 2)]

        test1 = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_routes=expected_routes,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal1 = rehearsing.WorkflowRehearsal(test1)
        rehearsal1.assert_conducting_sequences()

        # Rerun and complete workflow.
        expected_task_seq = [
            ("task1", 0),
            ("task2", 0),
            ("task3", 0),
            ("task4", 1),
            ("task4", 2),
            ("task5", 1),
            ("task5", 2),
            ("task6", 1),
            ("task6", 2),
            ("task5", 2),
            ("task7", 1),
            ("task7", 2),
        ]

        expected_term_tasks = [("task7", 1), ("task7", 2)]

        test2 = rehearsing.WorkflowRerunTestCase(
            rehearsal1.conductor,
            expected_task_seq,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal2 = rehearsing.WorkflowRehearsal(test2)
        rehearsal2.assert_conducting_sequences()

    def test_fail_at_multiple_splits(self):
        wf_name = "split"

        # Fail task5 at one of the split/fork and task6 in another split/fork.
        expected_routes = [
            [],  # default from start
            ["task2__t0"],  # task1 -> task2 -> task4
            ["task3__t0"],  # task1 -> task3 -> task4
        ]

        expected_task_seq = [
            ("task1", 0),
            ("task2", 0),
            ("task3", 0),
            ("task4", 1),
            ("task4", 2),
            ("task5", 1),
            ("task5", 2),
            ("task6", 1),
            ("task6", 2),
        ]

        mock_action_executions = [
            rehearsing.MockActionExecution("task5", seq_id=6, status=statuses.FAILED),
            rehearsing.MockActionExecution("task6", seq_id=7, status=statuses.FAILED),
        ]

        expected_term_tasks = [("task5", 2), ("task6", 1), ("task6", 2)]

        test1 = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_routes=expected_routes,
            expected_workflow_status=statuses.FAILED,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal1 = rehearsing.WorkflowRehearsal(test1)
        rehearsal1.assert_conducting_sequences()

        # Rerun and complete workflow.
        expected_task_seq = [
            ("task1", 0),
            ("task2", 0),
            ("task3", 0),
            ("task4", 1),
            ("task4", 2),
            ("task5", 1),
            ("task5", 2),
            ("task6", 1),
            ("task6", 2),
            ("task5", 2),
            ("task6", 1),
            ("task7", 1),
            ("task7", 2),
        ]

        expected_term_tasks = [("task7", 1), ("task7", 2)]

        test2 = rehearsing.WorkflowRerunTestCase(
            rehearsal1.conductor,
            expected_task_seq,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks,
        )

        rehearsal2 = rehearsing.WorkflowRehearsal(test2)
        rehearsal2.assert_conducting_sequences()
