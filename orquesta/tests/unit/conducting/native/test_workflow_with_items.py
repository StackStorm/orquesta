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
        wf_def = """
        version: 1.0
        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum
        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% succeeded() %>
                publish: items=<% result() %>
                do: task2
          task2:
            action: core.noop
        output:
          - items: <% ctx(items) %>
        """

        expected_task_seq = ["task1", "task2"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", result="fee", item_id=0),
            rehearsing.MockActionExecution("task1", result="fi", item_id=1),
            rehearsing.MockActionExecution("task1", result="fo", item_id=2),
            rehearsing.MockActionExecution("task1", result="fum", item_id=3),
        ]

        expected_output = {"items": ["fee", "fi", "fo", "fum"]}

        test = rehearsing.WorkflowTestCase(
            wf_def,
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_items_list_with_error(self):
        wf_def = """
        version: 1.0
        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum
        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% succeeded() %>
                publish: items=<% result() %>
                do: task2
          task2:
            action: core.noop
        output:
          - items: <% ctx(items) %>
        """

        expected_task_seq = ["task1"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", result="fee", item_id=0),
            rehearsing.MockActionExecution("task1", result="fi", item_id=1),
            rehearsing.MockActionExecution("task1", result="fo", item_id=2, status=statuses.FAILED),
        ]

        expected_output = None

        test = rehearsing.WorkflowTestCase(
            wf_def,
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_items_list_with_error_and_remediation(self):
        wf_def = """
        version: 1.0
        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum
        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% failed() %>
                publish: items=<% result() %>
                do: task2
          task2:
            action: core.noop
        output:
          - items: <% ctx(items) %>
        """

        expected_task_seq = ["task1", "task2"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", result="fee", item_id=0),
            rehearsing.MockActionExecution("task1", result="fi", item_id=1),
            rehearsing.MockActionExecution("task1", result=None, item_id=2, status=statuses.FAILED),
        ]

        expected_output = {"items": ["fee", "fi", None]}

        test = rehearsing.WorkflowTestCase(
            wf_def,
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.SUCCEEDED,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_parallel_items_tasks(self):
        wf_def = """
        version: 1.0
        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum
          - ys:
              - fie
              - foh
              - fum
        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% succeeded() %>
                publish: t1_items=<% result() %>
                do: task3
          task2:
            with: <% ctx(ys) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% succeeded() %>
                publish: t2_items=<% result() %>
                do: task3
          task3:
            join: all
            action: core.noop
        output:
          - t1_items: <% ctx(t1_items) %>
          - t2_items: <% ctx(t2_items) %>
        """

        expected_task_seq = ["task1", "task2", "task3"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", result="fee", item_id=0),
            rehearsing.MockActionExecution("task2", result="fie", item_id=0),
            rehearsing.MockActionExecution("task1", result="fi", item_id=1),
            rehearsing.MockActionExecution("task2", result="foh", item_id=1),
            rehearsing.MockActionExecution("task1", result="fo", item_id=2),
            rehearsing.MockActionExecution("task2", result="fum", item_id=2),
            rehearsing.MockActionExecution("task1", result="fum", item_id=3),
        ]

        expected_output = {
            "t1_items": ["fee", "fi", "fo", "fum"],
            "t2_items": ["fie", "foh", "fum"],
        }

        test = rehearsing.WorkflowTestCase(
            wf_def,
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()

    def test_parallel_items_tasks_with_error(self):
        wf_def = """
        version: 1.0
        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum
          - ys:
              - fie
              - foh
              - fum
        tasks:
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% succeeded() %>
                publish: t1_items=<% result() %>
                do: task3
          task2:
            with: <% ctx(ys) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% succeeded() %>
                publish: t2_items=<% result() %>
                do: task3
          task3:
            join: all
            action: core.noop
        output:
          - t1_items: <% ctx(t1_items) %>
          - t2_items: <% ctx(t2_items) %>
        """

        expected_task_seq = ["task1", "task2"]

        mock_action_executions = [
            rehearsing.MockActionExecution("task1", result="fee", item_id=0),
            rehearsing.MockActionExecution("task2", result="fie", item_id=0),
            rehearsing.MockActionExecution("task1", result="fi", item_id=1),
            rehearsing.MockActionExecution("task2", result="foh", item_id=1),
            rehearsing.MockActionExecution("task1", result=None, item_id=2, status=statuses.FAILED),
            rehearsing.MockActionExecution("task2", result="fum", item_id=2),
        ]

        expected_output = None

        test = rehearsing.WorkflowTestCase(
            wf_def,
            expected_task_seq,
            mock_action_executions=mock_action_executions,
            expected_workflow_status=statuses.FAILED,
            expected_output=expected_output,
        )

        rehearsing.WorkflowRehearsal(test).assert_conducting_sequences()
