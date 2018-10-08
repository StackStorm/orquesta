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


class WorkflowConductorWithItemsTest(base.WorkflowConductorTest):

    def assert_task_items(self, conductor, task_id, task_ctx, items, action_specs,
                          mock_ac_ex_states, expected_task_states, expected_workflow_states,
                          concurrency=None):

        # Set up test cases.
        tests = list(zip(mock_ac_ex_states, expected_task_states, expected_workflow_states))

        # Verify the first set of action executions.
        expected_task = self.format_task_item(
            task_id,
            task_ctx,
            conductor.spec.tasks.get_task(task_id),
            action_specs=action_specs[0:concurrency],
            items_count=len(items),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Mark the first set of action executions as running.
        for i in range(0, min(len(tests), concurrency or len(items))):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_id, ac_ex_event)

        # Ensure there are no actions when getting next tasks again.
        expected_tasks = []
        actual_tasks = conductor.get_next_tasks()
        self.assertListEqual(actual_tasks, expected_tasks)

        # Mock the action execution for each item.
        for i in range(0, len(tests)):
            context = {'item_id': i}
            result = items[i]
            ac_ex_state = tests[i][0]
            ac_ex_event = events.ActionExecutionEvent(ac_ex_state, result, context)
            conductor.update_task_flow(task_id, ac_ex_event)

            expected_task_state = tests[i][1]
            actual_task_state = conductor.get_task_flow_entry(task_id)['state']

            error_message = (
                'Task execution state "%s" does not match "%s" for item %s.' %
                (actual_task_state, expected_task_state, i)
            )

            self.assertEqual(actual_task_state, expected_task_state, error_message)

            expected_workflow_state = tests[i][2]
            actual_workflow_state = conductor.get_workflow_state()

            error_message = (
                'Workflow execution state "%s" does not match "%s" after item %s update.' %
                (actual_workflow_state, expected_workflow_state, i)
            )

            self.assertEqual(actual_workflow_state, expected_workflow_state, error_message)

            # Process next set of action executions only if there are more test cases.
            if i >= len(tests) - 2 or concurrency is None:
                continue

            item_id = i + concurrency
            expected_tasks = []

            if item_id < len(items):
                expected_task = self.format_task_item(
                    task_id,
                    task_ctx,
                    conductor.spec.tasks.get_task(task_id),
                    action_specs=action_specs[item_id:item_id + 1],
                    items_count=len(items),
                    items_concurrency=concurrency
                )

                expected_tasks = [expected_task]

            actual_tasks = conductor.get_next_tasks()
            self.assert_task_list(actual_tasks, expected_tasks)

            for task in actual_tasks:
                task_id = task['id']

                for action in task['actions']:
                    ctx = {'item_id': action['item_id']}
                    ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=ctx)
                    conductor.update_task_flow(task_id, ac_ex_event)

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
              - publish:
                  - items: <% result() %>

        output:
          - items: <% ctx(items) %>
        """

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Mock the action execution for each item and assert expected task states.
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_states = [states.SUCCEEDED] * 4
        expected_task_states = [states.RUNNING] * 3 + [states.SUCCEEDED]
        expected_workflow_states = [states.RUNNING] * 3 + [states.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_states,
            expected_task_states,
            expected_workflow_states
        )

        # Assert the task is removed from staging.
        self.assertNotIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

        # Assert the workflow output is correct.
        expected_output = {'items': task_ctx['xs']}
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_basic_items_list_with_concurrency(self):
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
            with:
              items: <% ctx(xs) %>
              concurrency: 2
            action: core.echo message=<% item() %>
            next:
              - publish:
                  - items: <% result() %>

        output:
          - items: <% ctx(items) %>
        """

        concurrency = 2

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Mock the action execution for each item and assert expected task states.
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_states = [states.SUCCEEDED] * 4
        expected_task_states = [states.RUNNING] * 3 + [states.SUCCEEDED]
        expected_workflow_states = [states.RUNNING] * 3 + [states.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_states,
            expected_task_states,
            expected_workflow_states,
            concurrency=concurrency
        )

        # Assert the task is removed from staging.
        self.assertNotIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_multiple_items_list(self):
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

        # Mock the action execution for each item and assert expected task states.
        task_name = 'task1'
        task_ctx = {'xs': ['foo', 'fu', 'marco'], 'ys': ['bar', 'bar', 'polo']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'foobar'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fubar'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'marcopolo'}, 'item_id': 2},
        ]

        mock_ac_ex_states = [states.SUCCEEDED] * 3
        expected_task_states = [states.RUNNING] * 2 + [states.SUCCEEDED]
        expected_workflow_states = [states.RUNNING] * 2 + [states.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_ctx,
            [i[0] + i[1] for i in zip(task_ctx['xs'], task_ctx['ys'])],
            task_action_specs,
            mock_ac_ex_states,
            expected_task_states,
            expected_workflow_states
        )

        # Assert the task is removed from staging.
        self.assertNotIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_multiple_items_list_with_concurrency(self):
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
            with:
              items: x, y in <% zip(ctx(xs), ctx(ys)) %>
              concurrency: 1
            action: core.echo message=<% item(x) + item(y) %>
        """

        concurrency = 1

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Mock the action execution for each item and assert expected task states.
        task_name = 'task1'
        task_ctx = {'xs': ['foo', 'fu', 'marco'], 'ys': ['bar', 'bar', 'polo']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'foobar'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fubar'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'marcopolo'}, 'item_id': 2},
        ]

        mock_ac_ex_states = [states.SUCCEEDED] * 3
        expected_task_states = [states.RUNNING] * 2 + [states.SUCCEEDED]
        expected_workflow_states = [states.RUNNING] * 2 + [states.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_ctx,
            [i[0] + i[1] for i in zip(task_ctx['xs'], task_ctx['ys'])],
            task_action_specs,
            mock_ac_ex_states,
            expected_task_states,
            expected_workflow_states,
            concurrency=concurrency
        )

        # Assert the task is removed from staging.
        self.assertNotIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_failed_item(self):
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

        # Mock the action execution for each item and assert expected task states.
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_states = [states.SUCCEEDED, states.FAILED, states.SUCCEEDED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING] + [states.FAILED] * 3
        expected_workflow_states = [states.RUNNING] + [states.FAILED] * 3

        self.assert_task_items(
            conductor,
            task_name,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_states,
            expected_task_states,
            expected_workflow_states
        )

        # Assert the task is removed from staging.
        self.assertNotIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)

    def test_failed_item_with_concurrency(self):
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
            with:
              items: <% ctx(xs) %>
              concurrency: 2
            action: core.echo message=<% item() %>
        """

        concurrency = 2

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Mock the action execution for each item and assert expected task states.
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_states = [states.SUCCEEDED, states.FAILED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING] + [states.FAILED] * 2
        expected_workflow_states = [states.RUNNING] + [states.FAILED] * 2

        self.assert_task_items(
            conductor,
            task_name,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_states,
            expected_task_states,
            expected_workflow_states,
            concurrency=concurrency
        )

        # Assert the task is removed from staging.
        self.assertNotIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)
