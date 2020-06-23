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

from orquesta import statuses
from orquesta.tests import exceptions as exc
import orquesta.tests.mocks as conduct_mock
from orquesta.tests.unit.base import WorkflowComposerTest
from orquesta.tests.unit.conducting.native import base


class BasicWorkflowConductorTest(base.OrchestraWorkflowConductorTest, WorkflowComposerTest):
    def test_sequential_fail(self):
        wf_name = "sequential_fail"

        expected_task_seq = ["task1", "task2", "task3", "fail"]

        mock_results = [
            "Stanley",
            "All your base are belong to us!",
            "Stanley, All your base are belong to us!",
        ]

        expected_output = {"greeting": mock_results[2]}
        expected_term_tasks = ["task3", "fail"]
        expected_workflow_status = statuses.FAILED
        expected_routes = [[]]

        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = conduct_mock.WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            inputs={"name": "Stanley"},
            mock_results=mock_results,
            mock_statuses=["succeeded", "succeeded", "succeeded", "succeeded"],
            expected_output=expected_output,
            expected_term_tasks=expected_term_tasks,
            expected_workflow_status=expected_workflow_status,
            expected_routes=expected_routes,
        )
        # will throw
        mock.assert_conducting_sequences()

    def test_sequential_routes_list(self):
        """Yaml does not support python tuple

        """
        wf_name = "sequential"

        expected_task_seq = [["task1", 0], ["task2", 0], ["task3", 0], ["continue", 0]]

        mock_results = [
            "Stanley",
            "All your base are belong to us!",
            "Stanley, All your base are belong to us!",
        ]

        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = conduct_mock.WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            inputs={"name": "Stanley"},
            mock_results=mock_results,
            mock_statuses=["succeeded", "succeeded", "succeeded", "succeeded"],
        )
        # will throw
        mock.assert_conducting_sequences()

    def test_sequential_term_task_throws(self):
        wf_name = "sequential"

        expected_task_seq = ["task1", "task2", "task3", "continue"]

        expected_workflow_status = statuses.SUCCEEDED
        expected_term_tasks = ["continue", "task3", "exception"]

        mock_results = [
            "Stanley",
            "All your base are belong to us!",
            "Stanley, All your base are belong to us!",
        ]

        expected_output = {"greeting": mock_results[2]}

        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = conduct_mock.WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            inputs={"name": "Stanley"},
            mock_results=mock_results,
            expected_workflow_status=expected_workflow_status,
            expected_output=expected_output,
            expected_term_tasks=expected_term_tasks,
        )
        # will throw
        self.assertRaises(exc.TermsEquality, mock.assert_conducting_sequences)

    def test_sequential_worfklowstatus_throws(self):
        wf_name = "sequential"

        expected_task_seq = ["task1", "task2", "task3", "continue"]

        expected_workflow_status = statuses.REQUESTED

        mock_results = [
            "Stanley",
            "All your base are belong to us!",
            "Stanley, All your base are belong to us!",
        ]

        expected_output = {"greeting": mock_results[2]}

        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = conduct_mock.WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            inputs={"name": "Stanley"},
            mock_results=mock_results,
            expected_workflow_status=expected_workflow_status,
            expected_output=expected_output,
        )
        # will throw
        self.assertRaises(exc.StatusEquality, mock.assert_conducting_sequences)

    def test_sequential_route_equality_throws(self):
        wf_name = "sequential"

        expected_task_seq = ["task1", "task2", "task3", "continue"]
        expected_routes = [[], ["wrong"]]

        mock_results = [
            "Stanley",
            "All your base are belong to us!",
            "Stanley, All your base are belong to us!",
        ]

        expected_output = {"greeting": mock_results[2]}

        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = conduct_mock.WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            inputs={"name": "Stanley"},
            expected_routes=expected_routes,
            mock_results=mock_results,
            expected_output=expected_output,
        )
        # will throw
        self.assertRaises(exc.RouteEquality, mock.assert_conducting_sequences)

    def test_sequential_task_equality_throws(self):
        wf_name = "sequential"

        expected_task_seq = ["task1", "task2", "task3", "continue", "exception"]

        mock_results = [
            "Stanley",
            "All your base are belong to us!",
            "Stanley, All your base are belong to us!",
        ]

        expected_output = {"greeting": mock_results[2]}

        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = conduct_mock.WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            inputs={"name": "Stanley"},
            mock_results=mock_results,
            expected_output=expected_output,
        )
        # will throw
        self.assertRaises(exc.TaskEquality, mock.assert_conducting_sequences)

    def test_sequential_task_output_throws(self):
        wf_name = "sequential"

        expected_task_seq = ["task1", "task2", "task3", "continue"]

        mock_results = [
            "Stanley",
            "All your base are belong to us!",
            "Stanley, All your base are belong to us!",
        ]

        expected_output = {"greeting": mock_results[2], "exception": True}

        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        mock = conduct_mock.WorkflowConductorMock(
            wf_spec,
            expected_task_seq,
            inputs={"name": "Stanley"},
            mock_results=mock_results,
            expected_output=expected_output,
        )
        # will throw
        self.assertRaises(exc.OutputEquality, mock.assert_conducting_sequences)
