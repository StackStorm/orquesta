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

from orquesta import conducting
from orquesta import exceptions as exc
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base


class WorkflowConductorErrorHandlingTest(test_base.WorkflowConductorTest):
    def test_workflow_input_error(self):
        wf_def = """
        version: 1.0

        input:
          - xyz: <% result().foobar %>

        tasks:
          task1:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% result().foobar %>'. ExpressionEvaluationException: "
                    "The current task is not set in the context."
                ),
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)

        self.assertRaises(
            exc.InvalidWorkflowStatusTransition, conductor.request_workflow_status, statuses.RUNNING
        )

        self.assertListEqual(conductor.get_next_tasks(), [])
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_workflow_input_seq_ref_error(self):
        wf_def = """
        version: 1.0

        input:
          - x
          - y: <% ctx().x %>
          - z: <% ctx().y.value %>

        tasks:
          task1:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().y.value %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#value"'
                ),
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)

        self.assertRaises(
            exc.InvalidWorkflowStatusTransition, conductor.request_workflow_status, statuses.RUNNING
        )

        self.assert_next_task(conductor, has_next_task=False)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_workflow_vars_error(self):
        wf_def = """
        version: 1.0

        vars:
          - xyz: <% result().foobar %>

        tasks:
          task1:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% result().foobar %>'. ExpressionEvaluationException: "
                    "The current task is not set in the context."
                ),
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)

        self.assertRaises(
            exc.InvalidWorkflowStatusTransition, conductor.request_workflow_status, statuses.RUNNING
        )

        self.assert_next_task(conductor, has_next_task=False)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_workflow_vars_seq_ref_error(self):
        wf_def = """
        version: 1.0

        vars:
          - x: 123
          - y: <% ctx().x %>
          - z: <% ctx().y.value %>

        tasks:
          task1:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().y.value %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#value"'
                ),
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)

        self.assertRaises(
            exc.InvalidWorkflowStatusTransition, conductor.request_workflow_status, statuses.RUNNING
        )

        self.assert_next_task(conductor, has_next_task=False)
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_task_transition_criteria_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% ctx().foobar.fubar %>
                publish:
                  - var1: 'xyz'
                do: task2
              - when: <% succeeded() %>
                do: task3
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var2: 123
                do: task3
          task3:
            join: all
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
                "task_transition_id": "task2__t0",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The workflow should fail on completion of task1 while evaluating task transition.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

        # There are two transitions in task1. The transition to task3 should be processed.
        staged_tasks = list(filter(lambda x: x["id"] == "task3", conductor.workflow_state.staged))
        self.assertGreater(len(staged_tasks), 0)

        # Since the workflow failed, there should be no next tasks returned.
        self.assert_next_task(conductor, has_next_task=False)

    def test_multiple_task_transition_criteria_errors(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar
          - fubar: foobar

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% ctx().foobar.fubar %>
                publish:
                  - var1: 'xyz'
                do: task3
          task2:
            action: core.noop
            next:
              - when: <% ctx().fubar.foobar %>
                publish:
                  - var2: 123
                do: task3
          task3:
            join: all
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
                "task_transition_id": "task3__t0",
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().fubar.foobar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#foobar"'
                ),
                "route": 0,
                "task_id": "task2",
                "task_transition_id": "task3__t0",
            },
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Manually complete task1 and task2. Although the workflow failed when
        # processing task1, task flow can still be updated for task2.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])
        self.forward_task_statuses(conductor, "task2", [statuses.RUNNING, statuses.SUCCEEDED])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

        # Since both tasks fail evaluating task transition, task3 should not be staged.
        self.assertNotIn("task3", conductor.workflow_state.staged)

        # Since the workflow failed, there should be no next tasks returned.
        self.assert_next_task(conductor, has_next_task=False)

    def test_task_transition_publish_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var1: <% ctx().foobar.fubar %>
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var2: 123
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
                "task_transition_id": "task2__t0",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The workflow should fail on completion of task1 while evaluating task transition.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)
        self.assertNotIn("task2", conductor.workflow_state.staged)

        # Since the workflow failed, there should be no next tasks returned.
        self.assert_next_task(conductor, has_next_task=False)

    def test_task_transition_publish_seq_ref_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - x: 123
                  - y: <% ctx().x %>
                  - z: <% ctx().y.value %>
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - var2: 123
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().y.value %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#value"'
                ),
                "route": 0,
                "task_id": "task1",
                "task_transition_id": "task2__t0",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The workflow should fail on completion of task1 while evaluating task transition.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)
        self.assertNotIn("task2", conductor.workflow_state.staged)

        # Since the workflow failed, there should be no next tasks returned.
        self.assert_next_task(conductor, has_next_task=False)

    def test_get_start_tasks_with_task_action_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: <% ctx().foobar.fubar %>
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The get_start_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_get_start_tasks_via_get_next_tasks_with_task_action_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: <% ctx().foobar.fubar %>
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The get_next_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_get_next_tasks_with_task_action_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: <% ctx().foobar.fubar %>
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task2",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Manually complete task1.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # The get_next_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_get_start_tasks_with_task_input_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            input:
              var_x: <% ctx().foobar.fubar %>
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The get_start_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_get_start_tasks_via_get_next_tasks_with_task_input_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            input:
              var_x: <% ctx().foobar.fubar %>
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The get_next_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_get_next_tasks_with_task_input_error(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
            input:
              var_x: <% ctx().foobar.fubar %>
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task2",
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Manually complete task1.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # The get_next_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_get_start_tasks_with_multiple_task_action_and_input_errors(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar
          - fubar: foobar

        tasks:
          task1:
            action: <% ctx().foobar.fubar %>
            next:
              - when: <% succeeded() %>
                do: task3
          task2:
            action: core.noop var_x=<% ctx().fubar.foobar %>
            next:
              - when: <% succeeded() %>
                do: task3
          task3:
            join: all
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().fubar.foobar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#foobar"'
                ),
                "route": 0,
                "task_id": "task2",
            },
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The get_start_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_get_start_tasks_via_get_next_tasks_with_multiple_task_action_and_input_errors(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar
          - fubar: foobar

        tasks:
          task1:
            action: <% ctx().foobar.fubar %>
            next:
              - when: <% succeeded() %>
                do: task3
          task2:
            action: core.noop var_x=<% ctx().fubar.foobar %>
            next:
              - when: <% succeeded() %>
                do: task3
          task3:
            join: all
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task1",
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().fubar.foobar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#foobar"'
                ),
                "route": 0,
                "task_id": "task2",
            },
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # The get_next_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_get_next_tasks_with_multiple_task_action_and_input_errors(self):
        wf_def = """
        version: 1.0

        description: A basic branching workflow.

        vars:
          - foobar: fubar
          - fubar: foobar

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2, task3
          task2:
            action: <% ctx().foobar.fubar %>
          task3:
            action: core.noop var_x=<% ctx().fubar.foobar %>
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().foobar.fubar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#fubar"'
                ),
                "route": 0,
                "task_id": "task2",
            },
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().fubar.foobar %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#foobar"'
                ),
                "route": 0,
                "task_id": "task3",
            },
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Manually complete task1.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # The get_next_tasks method should not return any tasks.
        self.assert_next_task(conductor, has_next_task=False)

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get("task_id", None))
        self.assertListEqual(actual_errors, expected_errors)

    def test_workflow_output_error(self):
        wf_def = """
        version: 1.0

        output:
          - xyz: <% result().foobar %>

        tasks:
          task1:
            action: core.noop
        """

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% result().foobar %>'. ExpressionEvaluationException: "
                    "The current task is not set in the context."
                ),
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Manually complete task1.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Render workflow output and checkout workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertIsNone(conductor.get_workflow_output())

    def test_workflow_output_seq_ref_error(self):
        wf_def = """
        version: 1.0

        output:
          - x: 123
          - y: <% ctx().x %>
          - z: <% ctx().y.value %>

        tasks:
          task1:
            action: core.noop
        """

        expected_output = {"x": 123, "y": 123}

        expected_errors = [
            {
                "type": "error",
                "message": (
                    "YaqlEvaluationException: Unable to evaluate expression "
                    "'<% ctx().y.value %>'. NoFunctionRegisteredException: "
                    'Unknown function "#property#value"'
                ),
            }
        ]

        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_status(statuses.RUNNING)

        # Manually complete task1.
        self.forward_task_statuses(conductor, "task1", [statuses.RUNNING, statuses.SUCCEEDED])

        # Render workflow output and checkout workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
