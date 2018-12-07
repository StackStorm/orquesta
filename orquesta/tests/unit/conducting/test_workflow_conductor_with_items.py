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


class WorkflowConductorWithItemsTest(base.WorkflowConductorWithItemsTest):

    def test_empty_items_list(self):
        wf_def = """
        version: 1.0

        vars:
          - xs: []

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
        task_ctx = {'xs': []}
        task_action_specs = []

        mock_ac_ex_states = []
        expected_task_states = [states.SUCCEEDED]
        expected_workflow_states = [states.SUCCEEDED]

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
        expected_output = {'items': []}
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

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
          - concurrency: 2
          - xs:
              - fee
              - fi
              - fo
              - fum

        tasks:
          task1:
            with:
              items: <% ctx(xs) %>
              concurrency: <% ctx(concurrency) %>
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
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum'], 'concurrency': 2}

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

    def test_failed_item_task_dormant(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.FAILED]
        expected_task_states = [states.RUNNING, states.FAILED]
        expected_workflow_states = [states.RUNNING, states.FAILED]

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

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)

    def test_failed_item_task_active(self):
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
        expected_task_states = [states.RUNNING] * 3 + [states.FAILED]
        expected_workflow_states = [states.RUNNING] * 3 + [states.FAILED]

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

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)

    def test_failed_item_task_dormant_with_concurrency(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.FAILED]
        expected_task_states = [states.RUNNING, states.FAILED]
        expected_workflow_states = [states.RUNNING, states.FAILED]

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

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)

    def test_failed_item_task_active_with_concurrency(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.FAILED, states.SUCCEEDED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING] * 3 + [states.FAILED]
        expected_workflow_states = [states.RUNNING] * 3 + [states.FAILED]

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

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_state(), states.FAILED)

    def test_cancel_item(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.CANCELED, states.SUCCEEDED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING] + [states.CANCELING] * 2 + [states.CANCELED]
        expected_workflow_states = [states.RUNNING] + [states.CANCELING] * 2 + [states.CANCELED]

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
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_cancel_with_items_incomplete(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.CANCELED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING, states.CANCELING, states.CANCELED]
        expected_workflow_states = [states.RUNNING, states.CANCELING, states.CANCELED]

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

        # Assert the workflow is canceled.
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_cancel_workflow_using_canceling_state_with_items_active(self):
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

        # Verify the set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs,
            items_count=len(task_ctx['xs'])
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_state(states.CANCELING)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELING)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.CANCELING)

        # Complete the items.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task is completed and workflow is canceled.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_cancel_workflow_using_canceled_state_with_items_active(self):
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

        # Verify the set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs,
            items_count=len(task_ctx['xs'])
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_state(states.CANCELED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELING)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.CANCELING)

        # Complete the items.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task is completed and workflow is canceled.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_cancel_workflow_using_canceling_state_with_items_concurrency(self):
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

        mock_ac_ex_states = [states.SUCCEEDED] * 2
        expected_task_states = [states.RUNNING] * 2
        expected_workflow_states = [states.RUNNING] * 2

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_state(states.CANCELING)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.CANCELED)

    def test_cancel_workflow_using_canceled_state_with_items_concurrency(self):
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

        mock_ac_ex_states = [states.SUCCEEDED] * 2
        expected_task_states = [states.RUNNING] * 2
        expected_workflow_states = [states.RUNNING] * 2

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_state(states.CANCELED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.CANCELED)

    def test_cancel_workflow_with_items_concurrency_and_active(self):
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

        # Verify the first set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs[0:concurrency],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0, concurrency):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_state(states.CANCELING)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELING)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.CANCELING)

        # Complete the items.
        for i in range(0, concurrency):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task and workflow are canceled.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.CANCELED)
        self.assertEqual(conductor.get_workflow_state(), states.CANCELED)

    def test_pause_item(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.PAUSED, states.SUCCEEDED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING, states.PAUSING, states.PAUSING, states.PAUSED]
        expected_workflow_states = [states.RUNNING, states.RUNNING, states.RUNNING, states.PAUSED]

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow is paused.
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

    def test_resume_paused_item(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.PAUSED, states.SUCCEEDED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING, states.PAUSING, states.PAUSING, states.PAUSED]
        expected_workflow_states = [states.RUNNING, states.RUNNING, states.RUNNING, states.PAUSED]

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume the paued action execution.
        context = {'item_id': 1}
        ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
        conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task and workflow is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)
        self.assertEqual(conductor.flow.staged[task_name]['items'][1]['state'], states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Complete the resumed action execution.
        context = {'item_id': 1}
        result = task_ctx['xs'][1]
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result=result, context=context)
        conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task is removed from staging.
        self.assertNotIn(task_name, conductor.flow.staged)

        # Assert the task and workflow succeeded.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_pause_workflow_using_pausing_state_with_items_active(self):
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

        # Verify the set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs,
            items_count=len(task_ctx['xs'])
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_state(states.PAUSING)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.PAUSING)

        # Complete the items.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task is completed and workflow is paused.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_state(states.RESUMING)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_pause_workflow_using_paused_state_with_items_active(self):
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

        # Verify the set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs,
            items_count=len(task_ctx['xs'])
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_state(states.PAUSED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.PAUSING)

        # Complete the items.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task is completed and workflow is paused.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_state(states.RESUMING)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_pause_workflow_using_pausing_state_with_items_concurrency(self):
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

        mock_ac_ex_states = [states.SUCCEEDED] * 2
        expected_task_states = [states.RUNNING] * 2
        expected_workflow_states = [states.RUNNING] * 2

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_state(states.PAUSING)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_state(states.RESUMING)
        self.assertEqual(conductor.get_workflow_state(), states.RESUMING)

        # Verify the second set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs[concurrency:],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Complete the items.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task and workflow are completed.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_pause_workflow_using_paused_state_with_items_concurrency(self):
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

        mock_ac_ex_states = [states.SUCCEEDED] * 2
        expected_task_states = [states.RUNNING] * 2
        expected_workflow_states = [states.RUNNING] * 2

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_state(states.PAUSED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_state(states.RESUMING)
        self.assertEqual(conductor.get_workflow_state(), states.RESUMING)

        # Verify the second set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs[concurrency:],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Complete the items.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task and workflow are completed.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_pause_workflow_with_items_concurrency_and_active(self):
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

        # Verify the first set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs[0:concurrency],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0, concurrency):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_state(states.PAUSING)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSING)
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.PAUSING)

        # Complete the items.
        for i in range(0, concurrency):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task and workflow are paused.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.PAUSED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_state(states.RESUMING)
        self.assertEqual(conductor.get_workflow_state(), states.RESUMING)

        # Verify the second set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            action_specs=task_action_specs[concurrency:],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(actual_tasks, expected_tasks)

        # Set the items to running state.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Complete the items.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            result = task_ctx['xs'][i]
            ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result, context=context)
            conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task and workflow are completed.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_pending_item(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.PENDING, states.SUCCEEDED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING, states.PAUSING, states.PAUSING, states.PAUSED]
        expected_workflow_states = [states.RUNNING, states.RUNNING, states.RUNNING, states.PAUSED]

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow is paused.
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

    def test_resume_pending_item(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.PENDING, states.SUCCEEDED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING, states.PAUSING, states.PAUSING, states.PAUSED]
        expected_workflow_states = [states.RUNNING, states.RUNNING, states.RUNNING, states.PAUSED]

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume the paued action execution.
        context = {'item_id': 1}
        ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
        conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task and workflow is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)
        self.assertEqual(conductor.flow.staged[task_name]['items'][1]['state'], states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Complete the resumed action execution.
        context = {'item_id': 1}
        result = task_ctx['xs'][1]
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result=result, context=context)
        conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task is removed from staging.
        self.assertNotIn(task_name, conductor.flow.staged)

        # Assert the task and workflow succeeded.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_resume_partial(self):
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

        mock_ac_ex_states = [states.SUCCEEDED, states.PAUSED, states.PAUSED, states.SUCCEEDED]
        expected_task_states = [states.RUNNING, states.PAUSING, states.PAUSING, states.PAUSED]
        expected_workflow_states = [states.RUNNING, states.RUNNING, states.RUNNING, states.PAUSED]

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

        # Assert the task is not removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

        # Resume the paued action execution.
        context = {'item_id': 1}
        ac_ex_event = events.ActionExecutionEvent(states.RUNNING, context=context)
        conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task and workflow is running.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.RUNNING)
        self.assertEqual(conductor.flow.staged[task_name]['items'][1]['state'], states.RUNNING)
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Complete the resumed action execution.
        context = {'item_id': 1}
        result = task_ctx['xs'][1]
        ac_ex_event = events.ActionExecutionEvent(states.SUCCEEDED, result=result, context=context)
        conductor.update_task_flow(task_name, ac_ex_event)

        # Assert the task is removed from staging.
        self.assertIn(task_name, conductor.flow.staged)

        # Assert the task and workflow is paused.
        self.assertEqual(conductor.flow.get_task(task_name)['state'], states.PAUSED)
        self.assertEqual(conductor.get_workflow_state(), states.PAUSED)

    def test_task_cycle(self):
        wf_def = """
        version: 1.0

        vars:
          - xs:
              - fee
              - fi
              - fo
              - fum

        tasks:
          init:
            next:
              - do: task1
          task1:
            with: <% ctx(xs) %>
            action: core.echo message=<% item() %>
            next:
              - when: <% failed() %>
                do: task1
        """

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        conductor = conducting.WorkflowConductor(spec)
        conductor.request_workflow_state(states.RUNNING)

        # Get pass the init task, required for bootstrapping self looping task..
        conductor.update_task_flow('init', events.ActionExecutionEvent(states.RUNNING))
        conductor.update_task_flow('init', events.ActionExecutionEvent(states.SUCCEEDED))

        # Mock the action execution for each item and assert expected task states.
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_states = [states.SUCCEEDED, states.FAILED]
        expected_task_states = [states.RUNNING, states.FAILED]
        expected_workflow_states = [states.RUNNING] * 2

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

        # Assert the task is reset in staging.
        self.assertIn(task_name, conductor.flow.staged)
        self.assertNotIn('items', conductor.flow.staged[task_name])

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_state(), states.RUNNING)

        # Mock the second task execution.
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
