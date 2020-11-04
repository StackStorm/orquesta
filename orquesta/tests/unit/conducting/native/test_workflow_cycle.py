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
        wf_name = "cycle"

        self.assert_spec_inspection(wf_name)

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

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_cycles(self):
        wf_name = "cycles"

        self.assert_spec_inspection(wf_name)

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

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_rollback_retry(self):
        wf_name = "rollback-retry"

        self.assert_spec_inspection(wf_name)

        expected_task_seq = [
            "init",
            "check",
            "create",
            "rollback",
            "check",
            "delete",
        ]

        mock_action_executions = [
            rehearsing.MockActionExecution("init"),
            rehearsing.MockActionExecution("check", status=statuses.FAILED),
            rehearsing.MockActionExecution("create"),
            rehearsing.MockActionExecution("rollback"),
            rehearsing.MockActionExecution("check"),
            rehearsing.MockActionExecution("delete"),
        ]

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_cycle_and_fork(self):
        wf_name = "cycle-fork"

        self.assert_spec_inspection(wf_name)

        expected_task_seq = [
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
        ]

        mock_action_executions = [
            rehearsing.MockActionExecution("init"),
            rehearsing.MockActionExecution("query", result=True),
            rehearsing.MockActionExecution("decide_cheer"),
            rehearsing.MockActionExecution("decide_work"),
            rehearsing.MockActionExecution("cheer"),
            rehearsing.MockActionExecution("notify_work"),
            rehearsing.MockActionExecution("toil"),
            rehearsing.MockActionExecution("query", result=False),
            rehearsing.MockActionExecution("decide_cheer"),
            rehearsing.MockActionExecution("decide_work"),
        ]

        test = rehearsing.WorkflowTestCase(
            self.get_wf_def(wf_name),
            expected_task_seq,
            mock_action_executions=mock_action_executions,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()
