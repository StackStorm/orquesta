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
from orquesta.specs import native as specs
from orquesta import statuses
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': []}
        task_action_specs = []

        mock_ac_ex_statuses = []
        expected_task_statuses = [statuses.SUCCEEDED]
        expected_workflow_statuses = [statuses.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 4
        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.SUCCEEDED]
        expected_workflow_statuses = [statuses.RUNNING] * 3 + [statuses.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum'], 'concurrency': 2}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 4
        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.SUCCEEDED]
        expected_workflow_statuses = [statuses.RUNNING] * 3 + [statuses.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['foo', 'fu', 'marco'], 'ys': ['bar', 'bar', 'polo']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'foobar'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fubar'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'marcopolo'}, 'item_id': 2},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 3
        expected_task_statuses = [statuses.RUNNING] * 2 + [statuses.SUCCEEDED]
        expected_workflow_statuses = [statuses.RUNNING] * 2 + [statuses.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            [i[0] + i[1] for i in zip(task_ctx['xs'], task_ctx['ys'])],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['foo', 'fu', 'marco'], 'ys': ['bar', 'bar', 'polo']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'foobar'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fubar'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'marcopolo'}, 'item_id': 2},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 3
        expected_task_statuses = [statuses.RUNNING] * 2 + [statuses.SUCCEEDED]
        expected_workflow_statuses = [statuses.RUNNING] * 2 + [statuses.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            [i[0] + i[1] for i in zip(task_ctx['xs'], task_ctx['ys'])],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_failed_item_task_dormant_other_incomplete(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING, statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_failed_item_task_dormant_other_failed(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING, statuses.RUNNING, statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.FAILED,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED
        ]

        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        expected_workflow_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING, statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.FAILED,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED
        ]

        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        expected_workflow_statuses = [statuses.RUNNING] * 3 + [statuses.FAILED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow failed.
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.CANCELED,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED
        ]

        expected_task_statuses = [
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED
        ]

        expected_workflow_statuses = [
            statuses.RUNNING,
            statuses.CANCELING,
            statuses.CANCELING,
            statuses.CANCELED
        ]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.CANCELED, statuses.SUCCEEDED]
        expected_task_statuses = [statuses.RUNNING, statuses.CANCELING, statuses.CANCELED]
        expected_workflow_statuses = [statuses.RUNNING, statuses.CANCELING, statuses.CANCELED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow is canceled.
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_cancel_workflow_using_canceling_status_with_items_active(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
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
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs,
            items_count=len(task_ctx['xs'])
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.CANCELING)

        # Complete the items.
        for i in range(0, len(task_ctx['xs'])):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task is completed and workflow is canceled.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_cancel_workflow_using_canceled_status_with_items_active(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
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
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs,
            items_count=len(task_ctx['xs'])
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_status(statuses.CANCELED)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.CANCELING)

        # Complete the items.
        for i in range(0, len(task_ctx['xs'])):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task is completed and workflow is canceled.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_cancel_workflow_using_canceling_status_with_items_concurrency(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 2
        expected_task_statuses = [statuses.RUNNING] * 2
        expected_workflow_statuses = [statuses.RUNNING] * 2

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.CANCELED)

    def test_cancel_workflow_using_canceled_status_with_items_concurrency(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 2
        expected_task_statuses = [statuses.RUNNING] * 2
        expected_workflow_statuses = [statuses.RUNNING] * 2

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_status(statuses.CANCELED)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.CANCELED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
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
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs[0:concurrency],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0, concurrency):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        # Cancel the workflow.
        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.CANCELING)

        # Complete the items.
        for i in range(0, concurrency):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task and workflow are canceled.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.CANCELED)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.PAUSED,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED
        ]

        expected_task_statuses = [
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSING,
            statuses.PAUSED
        ]

        expected_workflow_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED
        ]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow is paused.
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.PAUSED,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED
        ]

        expected_task_statuses = [
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSING,
            statuses.PAUSED
        ]

        expected_workflow_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED
        ]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the paued action execution.
        context = {'item_id': 1}
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert the task and workflow is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        staged_task = conductor.flow.get_staged_task(task_name, task_route)
        self.assertIsNotNone(staged_task)
        self.assertIn('items', staged_task)
        self.assertEqual(staged_task['items'][1]['status'], statuses.RUNNING)

        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Complete the resumed action execution.
        context = {'item_id': 1}
        result = task_ctx['xs'][1]
        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED], [context], [result])

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the task and workflow succeeded.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_pause_workflow_using_pausing_status_with_items_active(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
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
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs,
            items_count=len(task_ctx['xs'])
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.PAUSING)

        # Complete the items.
        for i in range(0, len(task_ctx['xs'])):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task and workflow are completed.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_status(statuses.RESUMING)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_pause_workflow_using_paused_status_with_items_active(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
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
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs,
            items_count=len(task_ctx['xs'])
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0, len(task_ctx['xs'])):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.PAUSING)

        # Complete the items.
        for i in range(0, len(task_ctx['xs'])):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task and workflow are completed.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_status(statuses.RESUMING)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_pause_workflow_using_pausing_status_with_items_concurrency(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 2
        expected_task_statuses = [statuses.RUNNING] * 2
        expected_workflow_statuses = [statuses.RUNNING] * 2

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_status(statuses.RESUMING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)

        # Verify the second set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs[concurrency:],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Complete the items.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task and workflow are completed.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

    def test_pause_workflow_using_paused_status_with_items_concurrency(self):
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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 2
        expected_task_statuses = [statuses.RUNNING] * 2
        expected_workflow_statuses = [statuses.RUNNING] * 2

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses,
            concurrency=concurrency
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_status(statuses.RESUMING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)

        # Verify the second set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs[concurrency:],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Complete the items.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task and workflow are completed.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
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
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs[0:concurrency],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0, concurrency):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        # Pause the workflow.
        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.PAUSING)

        # Complete the items.
        for i in range(0, concurrency):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task and workflow are paused.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.PAUSED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the workflow.
        conductor.request_workflow_status(statuses.RESUMING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)

        # Verify the second set of action executions.
        expected_task = self.format_task_item(
            task_name,
            task_route,
            task_ctx,
            conductor.spec.tasks.get_task(task_name),
            actions=task_action_specs[concurrency:],
            items_count=len(task_ctx['xs']),
            items_concurrency=concurrency
        )

        expected_tasks = [expected_task]
        actual_tasks = conductor.get_next_tasks()
        self.assert_task_list(conductor, actual_tasks, expected_tasks)

        # Set the items to running status.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            context = {'item_id': i}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert that the task is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Complete the items.
        for i in range(0 + concurrency, len(task_ctx['xs'])):
            contexts = [{'item_id': i}]
            results = [task_ctx['xs'][i]]
            status_changes = [statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, task_name, status_changes, contexts, results)

        # Assert the task and workflow are completed.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.PENDING,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED
        ]

        expected_task_statuses = [
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSING,
            statuses.PAUSED
        ]

        expected_workflow_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED
        ]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow is paused.
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.PENDING,
            statuses.SUCCEEDED,
            statuses.SUCCEEDED
        ]

        expected_task_statuses = [
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSING,
            statuses.PAUSED
        ]

        expected_workflow_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED
        ]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the paued action execution.
        context = {'item_id': 1}
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert the task and workflow is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        staged_task = conductor.flow.get_staged_task(task_name, task_route)
        self.assertEqual(staged_task['items'][1]['status'], statuses.RUNNING)

        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Complete the resumed action execution.
        context = {'item_id': 1}
        result = task_ctx['xs'][1]
        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED], [context], [result])

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the task and workflow succeeded.
        actual_task_status = conductor.flow.get_task(task_name, task_route)['status']
        self.assertEqual(actual_task_status, statuses.SUCCEEDED)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [
            statuses.SUCCEEDED,
            statuses.PAUSED,
            statuses.PAUSED,
            statuses.SUCCEEDED
        ]

        expected_task_statuses = [
            statuses.RUNNING,
            statuses.PAUSING,
            statuses.PAUSING,
            statuses.PAUSED
        ]

        expected_workflow_statuses = [
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.RUNNING,
            statuses.PAUSED
        ]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is not removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

        # Resume the paued action execution.
        context = {'item_id': 1}
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING], [context])

        # Assert the task and workflow is running.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.RUNNING)

        staged_task = conductor.flow.get_staged_task(task_name, task_route)
        self.assertEqual(staged_task['items'][1]['status'], statuses.RUNNING)

        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Complete the resumed action execution.
        context = {'item_id': 1}
        result = task_ctx['xs'][1]
        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED], [context], [result])

        # Assert the task is removed from staging.
        self.assertIsNotNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the task and workflow is paused.
        self.assertEqual(conductor.flow.get_task(task_name, task_route)['status'], statuses.PAUSED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

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
        conductor.request_workflow_status(statuses.RUNNING)

        # Get pass the init task, required for bootstrapping self looping task..
        self.forward_task_statuses(conductor, 'init', [statuses.RUNNING, statuses.SUCCEEDED])

        # Mock the action execution for each item and assert expected task statuses.
        task_route = 0
        task_name = 'task1'
        task_ctx = {'xs': ['fee', 'fi', 'fo', 'fum']}

        task_action_specs = [
            {'action': 'core.echo', 'input': {'message': 'fee'}, 'item_id': 0},
            {'action': 'core.echo', 'input': {'message': 'fi'}, 'item_id': 1},
            {'action': 'core.echo', 'input': {'message': 'fo'}, 'item_id': 2},
            {'action': 'core.echo', 'input': {'message': 'fum'}, 'item_id': 3},
        ]

        mock_ac_ex_statuses = [statuses.SUCCEEDED, statuses.FAILED]
        expected_task_statuses = [statuses.RUNNING, statuses.FAILED]
        expected_workflow_statuses = [statuses.RUNNING] * 2

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is reset in staging.
        staged_task = conductor.flow.get_staged_task(task_name, task_route)
        self.assertIsNotNone(staged_task)
        self.assertNotIn('items', staged_task)

        # Assert the workflow is still running.
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)

        # Mock the second task execution.
        mock_ac_ex_statuses = [statuses.SUCCEEDED] * 4
        expected_task_statuses = [statuses.RUNNING] * 3 + [statuses.SUCCEEDED]
        expected_workflow_statuses = [statuses.RUNNING] * 3 + [statuses.SUCCEEDED]

        self.assert_task_items(
            conductor,
            task_name,
            task_route,
            task_ctx,
            task_ctx['xs'],
            task_action_specs,
            mock_ac_ex_statuses,
            expected_task_statuses,
            expected_workflow_statuses
        )

        # Assert the task is removed from staging.
        self.assertIsNone(conductor.flow.get_staged_task(task_name, task_route))

        # Assert the workflow succeeded.
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
