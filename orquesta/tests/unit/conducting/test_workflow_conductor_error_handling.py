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
from orquesta import events
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base


class WorkflowConductorErrorHandlingTest(base.WorkflowConductorTest):

    def test_workflow_input_error(self):
        wf_def = """
        version: 1.0

        input:
          - xyz: <% result().foobar %>

        tasks:
          task1:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        self.assertListEqual(conductor.get_next_tasks(), [])
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)

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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().y.value %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#value"'
                )
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        self.assertListEqual(conductor.get_next_tasks(), [])
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
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

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        self.assertListEqual(conductor.get_next_tasks(), [])
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)

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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().y.value %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#value"'
                )
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        self.assertListEqual(conductor.get_next_tasks(), [])
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1',
                'task_transition_id': 'task2__0'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The workflow should fail on completion of task1 while evaluating task transition.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
        self.assertListEqual(actual_errors, expected_errors)

        # There are two transitions in task1. The transition to task3 should be processed.
        self.assertIn('task3', conductor.flow.staged)

        # Since the workflow failed, there should be no next tasks returned.
        self.assertListEqual(conductor.get_next_tasks(), [])

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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1',
                'task_transition_id': 'task3__0'
            },
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().fubar.foobar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#foobar"'
                ),
                'task_id': 'task2',
                'task_transition_id': 'task3__0'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Manually complete task1 and task2. Although the workflow failed when
        # processing task1, task flow can still be updated for task2.
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('task1', events.ActionExecutionEvent(states.SUCCEEDED))
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('task2', events.ActionExecutionEvent(states.SUCCEEDED))

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
        self.assertListEqual(actual_errors, expected_errors)

        # Since both tasks fail evaluating task transition, task3 should not be staged.
        self.assertNotIn('task3', conductor.flow.staged)

        # Since the workflow failed, there should be no next tasks returned.
        self.assertListEqual(conductor.get_next_tasks(), [])

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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1',
                'task_transition_id': 'task2__0'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The workflow should fail on completion of task1 while evaluating task transition.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
        self.assertListEqual(actual_errors, expected_errors)
        self.assertNotIn('task2', conductor.flow.staged)
        self.assertListEqual(conductor.get_next_tasks(), [])

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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().y.value %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#value"'
                ),
                'task_id': 'task1',
                'task_transition_id': 'task2__0'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The workflow should fail on completion of task1 while evaluating task transition.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
        self.assertListEqual(actual_errors, expected_errors)
        self.assertNotIn('task2', conductor.flow.staged)
        self.assertListEqual(conductor.get_next_tasks(), [])

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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The get_start_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The get_next_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task2'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Manually complete task1.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # The get_next_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The get_start_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The get_next_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task2'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Manually complete task1.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # The get_next_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1'
            },
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().fubar.foobar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#foobar"'
                ),
                'task_id': 'task2'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The get_start_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task1'
            },
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().fubar.foobar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#foobar"'
                ),
                'task_id': 'task2'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # The get_next_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().foobar.fubar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#fubar"'
                ),
                'task_id': 'task2'
            },
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().fubar.foobar %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#foobar"'
                ),
                'task_id': 'task3'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Manually complete task1.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        # The get_next_tasks method should not return any tasks.
        self.assertListEqual(conductor.get_next_tasks(), [])

        # The workflow should fail with the expected errors.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        actual_errors = sorted(conductor.errors, key=lambda x: x.get('task_id', None))
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
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% result().foobar %>\'. ExpressionEvaluationException: '
                    'The current task is not set in the context.'
                )
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Manually complete task1.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
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

        expected_output = {
            'x': 123,
            'y': 123
        }

        expected_errors = [
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression '
                    '\'<% ctx().y.value %>\'. NoFunctionRegisteredException: '
                    'Unknown function "#property#value"'
                )
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Manually complete task1.
        task_name = 'task1'
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)
