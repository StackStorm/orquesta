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
from orquesta import graphing
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base
from orquesta.utils import dictionary as dict_util
from orquesta.utils import jsonify as json_util


class WorkflowConductorTest(test_base.WorkflowConductorTest):
    def _prep_conductor(self, context=None, inputs=None, status=None):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        input:
          - a
          - b: False

        output:
          - data:
              a: <% ctx().a %>
              b: <% ctx().b %>
              c: <% ctx().c %>

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - c: 'xyz'
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
          task3:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task4
          task4:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task5
          task5:
            action: core.noop
        """

        spec = native_specs.WorkflowSpec(wf_def)

        kwargs = {
            "context": context if context is not None else None,
            "inputs": inputs if inputs is not None else None,
        }

        conductor = conducting.WorkflowConductor(spec, **kwargs)

        self.assertIsNone(conductor._graph)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)

        self.assertIsNone(conductor._workflow_state)
        self.assertIsInstance(conductor.workflow_state, conducting.WorkflowState)

        if status:
            self.assertEqual(conductor.workflow_state.status, statuses.UNSET)
            self.assertEqual(conductor.get_workflow_status(), statuses.UNSET)
            conductor.request_workflow_status(status)
            self.assertEqual(conductor.workflow_state.status, status)
            self.assertEqual(conductor.get_workflow_status(), status)
        else:
            self.assertEqual(conductor.workflow_state.status, statuses.UNSET)
            self.assertEqual(conductor.get_workflow_status(), statuses.UNSET)

        user_inputs = inputs or {}
        parent_context = context or {}

        self.assertDictEqual(conductor._inputs, user_inputs)
        self.assertDictEqual(conductor.get_workflow_input(), user_inputs)
        self.assertDictEqual(conductor._parent_ctx, parent_context)
        self.assertDictEqual(conductor.get_workflow_parent_context(), parent_context)

        default_inputs = {"a": None, "b": False}
        init_ctx_value = dict_util.merge_dicts(default_inputs, user_inputs, True)
        init_ctx_value = dict_util.merge_dicts(init_ctx_value, parent_context, True)
        self.assertDictEqual(conductor.get_workflow_initial_context(), init_ctx_value)

        return conductor

    def test_init(self):
        conductor = self._prep_conductor()

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            "spec": conductor.spec.serialize(),
            "graph": conductor.graph.serialize(),
            "context": {},
            "input": {},
            "output": None,
            "errors": [],
            "log": [],
            "state": {
                "status": statuses.UNSET,
                "routes": [[]],
                "staged": [
                    {"id": "task1", "route": 0, "prev": {}, "ctxs": {"in": [0]}, "ready": True}
                ],
                "tasks": {},
                "sequence": [],
                "contexts": [{"a": None, "b": False}],
            },
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, native_specs.WorkflowSpec)
        self.assertEqual(conductor.workflow_state.status, statuses.UNSET)
        self.assertEqual(conductor.get_workflow_status(), statuses.UNSET)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.workflow_state, conducting.WorkflowState)

    def test_init_with_inputs(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            "spec": conductor.spec.serialize(),
            "graph": conductor.graph.serialize(),
            "context": {},
            "input": inputs,
            "output": None,
            "errors": [],
            "log": [],
            "state": {
                "status": statuses.UNSET,
                "routes": [[]],
                "staged": [
                    {"id": "task1", "route": 0, "prev": {}, "ctxs": {"in": [0]}, "ready": True}
                ],
                "tasks": {},
                "sequence": [],
                "contexts": [inputs],
            },
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, native_specs.WorkflowSpec)
        self.assertEqual(conductor.workflow_state.status, statuses.UNSET)
        self.assertEqual(conductor.get_workflow_status(), statuses.UNSET)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.workflow_state, conducting.WorkflowState)

    def test_init_with_partial_inputs(self):
        inputs = {"a": 123}
        default_inputs = {"b": False}
        expected_initial_ctx = dict_util.merge_dicts(inputs, default_inputs, True)
        conductor = self._prep_conductor(inputs=inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            "spec": conductor.spec.serialize(),
            "graph": conductor.graph.serialize(),
            "context": {},
            "input": inputs,
            "output": None,
            "errors": [],
            "log": [],
            "state": {
                "status": statuses.UNSET,
                "routes": [[]],
                "staged": [
                    {"id": "task1", "route": 0, "prev": {}, "ctxs": {"in": [0]}, "ready": True}
                ],
                "tasks": {},
                "sequence": [],
                "contexts": [expected_initial_ctx],
            },
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, native_specs.WorkflowSpec)
        self.assertEqual(conductor.workflow_state.status, statuses.UNSET)
        self.assertEqual(conductor.get_workflow_status(), statuses.UNSET)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.workflow_state, conducting.WorkflowState)

    def test_init_with_context(self):
        context = {"parent": {"ex_id": "12345"}}
        inputs = {"a": 123, "b": True}
        init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), json_util.deepcopy(context))

        conductor = self._prep_conductor(context=context, inputs=inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            "spec": conductor.spec.serialize(),
            "graph": conductor.graph.serialize(),
            "context": context,
            "input": inputs,
            "output": None,
            "errors": [],
            "log": [],
            "state": {
                "status": statuses.UNSET,
                "routes": [[]],
                "staged": [
                    {"id": "task1", "route": 0, "prev": {}, "ctxs": {"in": [0]}, "ready": True}
                ],
                "tasks": {},
                "sequence": [],
                "contexts": [init_ctx],
            },
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, native_specs.WorkflowSpec)
        self.assertEqual(conductor.workflow_state.status, statuses.UNSET)
        self.assertEqual(conductor.get_workflow_status(), statuses.UNSET)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.workflow_state, conducting.WorkflowState)

    def test_serialization(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        # Mock task flows.
        for i in range(1, 6):
            status_changes = [statuses.RUNNING, statuses.SUCCEEDED]
            self.forward_task_statuses(conductor, "task" + str(i), status_changes)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            "spec": conductor.spec.serialize(),
            "graph": conductor.graph.serialize(),
            "state": conductor.workflow_state.serialize(),
            "context": conductor.get_workflow_parent_context(),
            "input": conductor.get_workflow_input(),
            "output": conductor.get_workflow_output(),
            "errors": conductor.errors,
            "log": conductor.log,
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, native_specs.WorkflowSpec)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertIsInstance(conductor.workflow_state, conducting.WorkflowState)
        self.assertEqual(len(conductor.workflow_state.tasks), 5)
        self.assertEqual(len(conductor.workflow_state.sequence), 5)

    def test_get_workflow_initial_context(self):
        conductor = self._prep_conductor()
        expected_init_ctx = {"a": None, "b": False}
        self.assertDictEqual(conductor.get_workflow_initial_context(), expected_init_ctx)

    def test_get_workflow_initial_context_with_inputs(self):
        inputs = {"a": 123, "b": True}
        expected_init_ctx = inputs
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)
        self.assertDictEqual(conductor.get_workflow_initial_context(), expected_init_ctx)

    def test_get_start_tasks(self):
        inputs = {"a": 123}
        expected_task_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)
        self.assert_next_task(conductor, "task1", expected_task_ctx)

    def test_get_start_tasks_when_graph_paused(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        conductor.request_workflow_status(statuses.PAUSING)
        self.assertListEqual(conductor.get_next_tasks(), [])

        conductor.request_workflow_status(statuses.PAUSED)
        self.assertListEqual(conductor.get_next_tasks(), [])

    def test_get_start_tasks_when_graph_canceled(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        conductor.request_workflow_status(statuses.CANCELING)
        self.assertListEqual(conductor.get_next_tasks(), [])

        conductor.request_workflow_status(statuses.CANCELED)
        self.assertListEqual(conductor.get_next_tasks(), [])

    def test_get_start_tasks_when_graph_abended(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        conductor.request_workflow_status(statuses.FAILED)
        self.assertListEqual(conductor.get_next_tasks(), [])

    def test_get_task(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_route = 0
        task_name = "task1"
        expected_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        expected_ctx["__current_task"] = {"id": task_name, "route": task_route}
        expected_ctx["__state"] = conductor.workflow_state.serialize()
        task = conductor.get_task(task_name, task_route)
        self.assertEqual(task["id"], task_name)
        self.assertEqual(task["route"], task_route)
        self.assertDictEqual(task["ctx"], expected_ctx)

        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        task_name = "task2"
        expected_ctx = dict_util.merge_dicts(json_util.deepcopy(expected_ctx), {"c": "xyz"})
        expected_ctx["__current_task"] = {"id": task_name, "route": task_route}
        expected_ctx["__state"] = conductor.workflow_state.serialize()
        task = conductor.get_task(task_name, task_route)
        self.assertEqual(task["id"], task_name)
        self.assertEqual(task["route"], task_route)
        self.assertDictEqual(task["ctx"], expected_ctx)

    def test_get_next_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        for i in range(1, 5):
            task_name = "task" + str(i)
            next_task_name = "task" + str(i + 1)
            expected_task_ctx = {"a": 123, "b": False, "c": "xyz"}
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
            self.assert_next_task(conductor, next_task_name, expected_task_ctx)

    def test_get_next_tasks_repeatedly(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        next_task_name = "task2"
        expected_init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})

        self.assert_next_task(conductor, task_name, expected_init_ctx)

        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assert_next_task(conductor, has_next_task=False)

        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        self.forward_task_statuses(conductor, next_task_name, [statuses.RUNNING])
        self.assert_next_task(conductor, has_next_task=False)
        self.assert_next_task(conductor, has_next_task=False)

        self.forward_task_statuses(conductor, task_name, [statuses.SUCCEEDED])
        self.assert_next_task(conductor, has_next_task=False)
        self.assert_next_task(conductor, has_next_task=False)

    def test_get_next_tasks_when_this_task_paused(self):
        inputs = {"a": 123}
        expected_init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        task_name = "task2"
        next_task_name = "task3"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.PAUSING])
        self.assert_next_task(conductor, has_next_task=False)
        self.forward_task_statuses(conductor, task_name, [statuses.PAUSED])
        self.assert_next_task(conductor, has_next_task=False)

        # After the previous task is paused, since there is no other tasks running,
        # the workflow is paused. The workflow needs to be resumed manually.
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)
        conductor.request_workflow_status(statuses.RESUMING)
        self.assertEqual(conductor.get_workflow_status(), statuses.RESUMING)

        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

    def test_get_next_tasks_when_graph_paused(self):
        inputs = {"a": 123}
        expected_init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        conductor.request_workflow_status(statuses.PAUSING)
        self.assert_next_task(conductor, has_next_task=False)

        conductor.request_workflow_status(statuses.PAUSED)
        self.assert_next_task(conductor, has_next_task=False)

        conductor.request_workflow_status(statuses.RESUMING)
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

    def test_get_next_tasks_when_this_task_canceled(self):
        inputs = {"a": 123}
        expected_init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        task_name = "task2"
        next_task_name = "task3"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.CANCELING])
        self.assert_next_task(conductor, has_next_task=False)
        self.forward_task_statuses(conductor, task_name, [statuses.CANCELED])
        self.assert_next_task(conductor, has_next_task=False)

        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_get_next_tasks_when_graph_canceled(self):
        inputs = {"a": 123}
        expected_init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        conductor.request_workflow_status(statuses.CANCELING)
        self.assert_next_task(conductor, has_next_task=False)

        conductor.request_workflow_status(statuses.CANCELED)
        self.assert_next_task(conductor, has_next_task=False)

    def test_get_next_tasks_when_this_task_abended(self):
        inputs = {"a": 123}
        expected_init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        task_name = "task2"
        next_task_name = "task3"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.FAILED])
        self.assert_next_task(conductor, has_next_task=False)

        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

    def test_get_next_tasks_when_graph_abended(self):
        inputs = {"a": 123}
        expected_init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])
        self.assert_next_task(conductor, next_task_name, expected_task_ctx)

        conductor.request_workflow_status(statuses.FAILED)
        self.assert_next_task(conductor, has_next_task=False)

    def test_get_task_initial_context(self):
        inputs = {"a": 123}
        expected_init_ctx = dict_util.merge_dicts(json_util.deepcopy(inputs), {"b": False})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_route = 0
        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        actual_task_ctx = conductor.get_task_initial_context(task_name, task_route)
        self.assertDictEqual(actual_task_ctx, expected_init_ctx)

        expected_task_out_ctx = {"c": "xyz"}
        expected_context_list = [expected_init_ctx, expected_task_out_ctx]
        self.assertListEqual(conductor.workflow_state.contexts, expected_context_list)

    def test_get_task_transition_contexts(self):
        inputs = {"a": 123, "b": True}
        expected_init_ctx = json_util.deepcopy(inputs)
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        # Get context for task2 that is staged by not yet running.
        task_route = 0
        task_name = "task1"
        next_task_name = "task2"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        next_task_in_ctx = expected_task_ctx
        expected_task_transition_ctx = {"%s__t0" % next_task_name: next_task_in_ctx}

        self.assertDictEqual(
            conductor.get_task_transition_contexts(task_name, task_route),
            expected_task_transition_ctx,
        )

        # Get context for task2 that is already running.
        task_name = "task2"
        next_task_name = "task3"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        expected_task_transition_ctx = {}

        self.assertDictEqual(
            conductor.get_task_transition_contexts(task_name, task_route),
            expected_task_transition_ctx,
        )

        # Get context for task3 that is not staged yet.
        self.assertDictEqual(conductor.get_task_transition_contexts(task_name, task_route), {})

        # Get transition context for task3 that has not yet run.
        self.assertRaises(
            exc.InvalidTaskStateEntry,
            conductor.get_task_transition_contexts,
            next_task_name,
            task_route,
        )

    def test_get_terminal_tasks_when_workflow_incomplete(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        for i in range(1, 5):
            task_name = "task" + str(i)
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertListEqual(conductor.workflow_state.get_terminal_tasks(), [])

    def test_get_terminal_tasks_when_workflow_completed(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        for i in range(1, 6):
            task_name = "task" + str(i)
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)

        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = [t["id"] for i, t in term_tasks]
        expected_term_tasks = ["task5"]
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

    def test_get_terminal_tasks_when_workflow_failed(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        for i in range(1, 4):
            task_name = "task" + str(i)
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        task_name = "task4"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.FAILED])

        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)

        term_tasks = conductor.workflow_state.get_terminal_tasks()
        actual_term_tasks = [t["id"] for i, t in term_tasks]
        expected_term_tasks = ["task4"]
        self.assertListEqual(actual_term_tasks, expected_term_tasks)

    def test_get_workflow_terminal_context_when_workflow_incomplete(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)
        self.assertRaises(exc.WorkflowContextError, conductor.get_workflow_terminal_context)

    def test_get_workflow_terminal_context_when_workflow_completed(self):
        inputs = {"a": 123, "b": True}
        expected_init_ctx = json_util.deepcopy(inputs)
        expected_term_ctx = json_util.deepcopy(expected_init_ctx)
        expected_term_ctx = dict_util.merge_dicts(expected_term_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        for i in range(1, 6):
            task_name = "task" + str(i)
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_terminal_context(), expected_term_ctx)

    def test_get_workflow_output_when_workflow_incomplete(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        for i in range(1, 5):
            task_name = "task" + str(i)
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertIsNone(conductor.get_workflow_output())

    def test_get_workflow_output_when_workflow_failed(self):
        inputs = {"a": 123, "b": True}
        expected_init_ctx = json_util.deepcopy(inputs)
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        for i in range(1, 5):
            task_name = "task" + str(i)
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        task_name = "task5"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.FAILED])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        expected_output = {"data": expected_task_ctx}
        self.assertEqual(conductor.get_workflow_status(), statuses.FAILED)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_get_workflow_output_when_workflow_succeeded(self):
        inputs = {"a": 123, "b": True}
        expected_init_ctx = json_util.deepcopy(inputs)
        expected_task_ctx = json_util.deepcopy(expected_init_ctx)
        expected_task_ctx = dict_util.merge_dicts(expected_task_ctx, {"c": "xyz"})
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        for i in range(1, 6):
            task_name = "task" + str(i)
            self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        # Render workflow output and check workflow status and output.
        conductor.render_workflow_output()
        expected_output = {"data": expected_task_ctx}
        self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_set_workflow_canceling_when_no_active_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_set_workflow_canceled_when_no_active_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        conductor.request_workflow_status(statuses.CANCELED)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELED)

    def test_set_workflow_canceling_when_has_active_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        conductor.request_workflow_status(statuses.CANCELING)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

    def test_set_workflow_canceled_when_has_active_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        conductor.request_workflow_status(statuses.CANCELED)
        self.assertEqual(conductor.get_workflow_status(), statuses.CANCELING)

    def test_set_workflow_pausing_when_no_active_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

    def test_set_workflow_paused_when_no_active_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING, statuses.SUCCEEDED])

        conductor.request_workflow_status(statuses.PAUSED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSED)

    def test_set_workflow_pausing_when_has_active_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        conductor.request_workflow_status(statuses.PAUSING)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

    def test_set_workflow_paused_when_has_active_tasks(self):
        inputs = {"a": 123}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        task_name = "task1"
        self.forward_task_statuses(conductor, task_name, [statuses.RUNNING])

        conductor.request_workflow_status(statuses.PAUSED)
        self.assertEqual(conductor.get_workflow_status(), statuses.PAUSING)

    def test_append_log_entries(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        extra = {"x": 1234}
        conductor.log_entry("info", "The workflow is running as expected.", data=extra)
        conductor.log_entry("warn", "The task may be running a little bit slow.", task_id="task1")
        conductor.log_entry("error", "This is baloney.", task_id="task1")
        conductor.log_error(TypeError("Something is not right."), task_id="task1")
        conductor.log_errors([KeyError("task1"), ValueError("foobar")], task_id="task1")

        self.assertRaises(
            exc.WorkflowLogEntryError, conductor.log_entry, "foobar", "This is foobar."
        )

        expected_log_entries = [
            {"type": "info", "message": "The workflow is running as expected.", "data": extra},
            {
                "type": "warn",
                "message": "The task may be running a little bit slow.",
                "task_id": "task1",
            },
        ]

        expected_errors = [
            {"type": "error", "message": "This is baloney.", "task_id": "task1"},
            {"type": "error", "message": "TypeError: Something is not right.", "task_id": "task1"},
            {"type": "error", "message": "KeyError: 'task1'", "task_id": "task1"},
            {"type": "error", "message": "ValueError: foobar", "task_id": "task1"},
        ]

        self.assertListEqual(conductor.log, expected_log_entries)
        self.assertListEqual(conductor.errors, expected_errors)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            "spec": conductor.spec.serialize(),
            "graph": conductor.graph.serialize(),
            "context": {},
            "input": inputs,
            "output": None,
            "errors": expected_errors,
            "log": expected_log_entries,
            "state": {
                "status": statuses.RUNNING,
                "routes": [[]],
                "staged": [
                    {"id": "task1", "route": 0, "prev": {}, "ctxs": {"in": [0]}, "ready": True}
                ],
                "tasks": {},
                "sequence": [],
                "contexts": [inputs],
            },
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, native_specs.WorkflowSpec)
        self.assertEqual(conductor.get_workflow_status(), statuses.RUNNING)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.workflow_state, conducting.WorkflowState)
        self.assertListEqual(conductor.log, expected_log_entries)
        self.assertListEqual(conductor.errors, expected_errors)

    def test_append_duplicate_log_entries(self):
        inputs = {"a": 123, "b": True}
        conductor = self._prep_conductor(inputs=inputs, status=statuses.RUNNING)

        extra = {"x": 1234}
        task = {"task_id": "task1", "route": 0}
        conductor.log_entry("info", "The workflow is running as expected.", data=extra)
        conductor.log_entry("info", "The workflow is running as expected.", data=extra)
        conductor.log_entry("warn", "The task may be running a little bit slow.", **task)
        conductor.log_entry("warn", "The task may be running a little bit slow.", **task)
        conductor.log_entry("error", "This is baloney.", **task)
        conductor.log_entry("error", "This is baloney.", **task)
        conductor.log_error(TypeError("Something is not right."), **task)
        conductor.log_error(TypeError("Something is not right."), **task)
        conductor.log_errors([KeyError("task1"), ValueError("foobar")], **task)
        conductor.log_errors([KeyError("task1"), ValueError("foobar")], **task)

        expected_log_entries = [
            {"type": "info", "message": "The workflow is running as expected.", "data": extra},
            {
                "type": "warn",
                "message": "The task may be running a little bit slow.",
                "task_id": "task1",
                "route": 0,
            },
        ]

        expected_errors = [
            {"type": "error", "message": "This is baloney.", "task_id": "task1", "route": 0},
            {
                "type": "error",
                "message": "TypeError: Something is not right.",
                "task_id": "task1",
                "route": 0,
            },
            {"type": "error", "message": "KeyError: 'task1'", "task_id": "task1", "route": 0},
            {"type": "error", "message": "ValueError: foobar", "task_id": "task1", "route": 0},
        ]

        self.assertListEqual(conductor.log, expected_log_entries)
        self.assertListEqual(conductor.errors, expected_errors)
