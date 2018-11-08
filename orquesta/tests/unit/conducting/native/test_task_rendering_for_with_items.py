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


class WorkflowConductorWithItemsTaskRenderingTest(base.WorkflowConductorTest):

    def test_bad_item_key(self):
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
            with: x in <% ctx(xs) %>
            action: core.echo message=<% item(y) %>
        """

        expected_errors = [
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression \'<% item(y) %>\'. '
                    'ExpressionEvaluationException: Item does not have key "y".'
                ),
                'task_id': 'task1'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)
        tasks = conductor.get_next_tasks()

        self.assertListEqual(tasks, [])
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_bad_item_type(self):
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
            action: core.echo message=<% item(x) %>
        """

        expected_errors = [
            {
                'type': 'error',
                'message': (
                    'YaqlEvaluationException: Unable to evaluate expression \'<% item(x) %>\'. '
                    'ExpressionEvaluationException: Item is not type of dict.'
                ),
                'task_id': 'task1'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)
        tasks = conductor.get_next_tasks()

        self.assertListEqual(tasks, [])
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_bad_items_type(self):
        wf_def = """
        version: 1.0

        vars:
          - xs: fee fi fo fum

        tasks:
          task1:
            with: x in <% ctx(xs) %>
            action: core.echo message=<% item(y) %>
        """

        expected_errors = [
            {
                'type': 'error',
                'message': 'TypeError: The value of "<% ctx(xs) %>" is not type of list.',
                'task_id': 'task1'
            }
        ]

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)
        tasks = conductor.get_next_tasks()

        self.assertListEqual(tasks, [])
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_start_task_rendering(self):
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
            with: x in <% ctx(xs) %>
            action: core.echo message=<% item(x) %>
        """

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        next_task_name = 'task1'
        next_task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            next_task_ctx,
            next_task_spec,
            action_specs=next_task_action_specs,
            items_count=len(next_task_ctx['xs']),
            items_concurrency=None
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

    def test_next_task_rendering(self):
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
            action: core.noop
            next:
              - do: task2
          task2:
            with: x in <% ctx(xs) %>
            action: core.echo message=<% item(x) %>
        """

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Process start task.
        next_task_name = 'task1'
        next_task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {'action': 'core.noop', 'input': None}
        ]

        expected_task = self.format_task_item(
            next_task_name,
            next_task_ctx,
            next_task_spec,
            action_specs=next_task_action_specs
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        conductor.update_task_flow(next_task_name, events.ActionExecutionEvent(states.RUNNING))
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED)
        conductor.update_task_flow(next_task_name, ac_ex_event)

        # Process next task.
        next_task_name = 'task2'
        next_task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            next_task_ctx,
            next_task_spec,
            action_specs=next_task_action_specs,
            items_count=len(next_task_ctx['xs']),
            items_concurrency=None
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

    def test_basic_list_rendering(self):
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
        """

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        next_task_name = 'task1'
        next_task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            next_task_ctx,
            next_task_spec,
            action_specs=next_task_action_specs,
            items_count=len(next_task_ctx['xs']),
            items_concurrency=None
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

    def test_multiple_lists_rendering(self):
        wf_def = """
        version: 1.0

        vars:
          - xs:
              - foo
              - fu
              - marco
          - ys:
              - bar
              - bar
              - polo

        tasks:
          task1:
            with: x, y in <% zip(ctx(xs), ctx(ys)) %>
            action: core.echo message=<% item(x) + item(y) %>
        """

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        next_task_name = 'task1'
        next_task_ctx = {'xs': ['foo', 'fu', 'marco'], 'ys': ['bar', 'bar', 'polo']}
        next_task_spec = conductor.spec.tasks.get_task(next_task_name)

        next_task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'foobar'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fubar'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'marcopolo'}, 'item_id': 2},
        ]

        expected_task = self.format_task_item(
            next_task_name,
            next_task_ctx,
            next_task_spec,
            action_specs=next_task_action_specs,
            items_count=len(next_task_ctx['xs']),
            items_concurrency=None
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)
