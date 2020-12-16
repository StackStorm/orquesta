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
from orquesta import rehearsing
from orquesta import statuses
from orquesta.tests.unit.specs.native import base as test_base


class WorkflowRehearsalSpecTest(test_base.OrchestraWorkflowSpecTest):
    def test_load_mock_action_execution_dict_minimal(self):
        ac_ex_spec = {"task_id": "task1"}
        ac_ex = rehearsing.MockActionExecution(ac_ex_spec)

        self.assertEqual(ac_ex.task_id, ac_ex_spec["task_id"])
        self.assertEqual(ac_ex.route, 0)
        self.assertIsNone(ac_ex.seq_id)
        self.assertIsNone(ac_ex.item_id)
        self.assertEqual(ac_ex.iter_id, 0)
        self.assertEqual(ac_ex.num_iter, 1)
        self.assertEqual(ac_ex.status, statuses.SUCCEEDED)
        self.assertIsNone(ac_ex.result)
        self.assertEqual(ac_ex.iter_pos, -1)

    def test_load_test_case_dict_minimal(self):
        test_case_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
        }

        test_case = rehearsing.load_test_case(test_case_spec)

        self.assertIsInstance(test_case, rehearsing.WorkflowTestCase)
        self.assertEqual(test_case.workflow, test_case_spec["workflow"])

        self.assertListEqual(
            test_case.expected_task_sequence, test_case_spec["expected_task_sequence"]
        )

        self.assertTrue(test_case.wf_def.startswith("version"))
        self.assertDictEqual(test_case.inputs, {})
        self.assertDictEqual(test_case.expected_inspection_errors, {})
        self.assertListEqual(test_case.expected_routes, [[]])
        self.assertListEqual(test_case.mock_action_executions, [])
        self.assertEqual(test_case.expected_workflow_status, statuses.SUCCEEDED)
        self.assertIsNone(test_case.expected_term_tasks)
        self.assertIsNone(test_case.expected_errors)
        self.assertIsNone(test_case.expected_output)

    def test_load_test_case_dict_with_mock_action_executions(self):
        test_case_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [{"task_id": "task1"}, {"task_id": "task2"}],
        }

        test_case = rehearsing.load_test_case(test_case_spec)

        self.assertEqual(len(test_case.mock_action_executions), 2)
        self.assertEqual(test_case.mock_action_executions[0].task_id, "task1")
        self.assertEqual(test_case.mock_action_executions[1].task_id, "task2")

        for ac_ex in test_case.mock_action_executions:
            self.assertIsInstance(ac_ex, rehearsing.MockActionExecution)
            self.assertEqual(ac_ex.route, 0)
            self.assertIsNone(ac_ex.seq_id)
            self.assertIsNone(ac_ex.item_id)
            self.assertEqual(ac_ex.iter_id, 0)
            self.assertEqual(ac_ex.num_iter, 1)
            self.assertEqual(ac_ex.status, statuses.SUCCEEDED)
            self.assertIsNone(ac_ex.result)

    def test_load_test_case_dict_with_expected_inspection_errors(self):
        test_case_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "expected_inspection_errors": {
                "syntax": [
                    {
                        "message": "['foobar'] is not of type 'string'",
                        "schema_path": (
                            "properties.tasks.patternProperties.^\\w+$."
                            "properties.next.items.properties.when.type"
                        ),
                        "spec_path": "tasks.task1.next[0].when",
                    }
                ],
                "context": [
                    {
                        "type": "yaql",
                        "expression": '<% ctx("__xyz") %>',
                        "spec_path": "output[0]",
                        "schema_path": "properties.output",
                        "message": (
                            'Variable "__xyz" that is prefixed with double underscores '
                            "is considered a private variable and cannot be referenced."
                        ),
                    },
                ],
            },
        }

        test_case = rehearsing.load_test_case(test_case_spec)

        self.assertEqual(len(test_case.expected_inspection_errors.syntax), 1)
        syntax_error = test_case.expected_inspection_errors.syntax[0]
        self.assertIsNone(syntax_error.type)
        self.assertIsNone(syntax_error.expression)
        self.assertEqual(syntax_error.message, "['foobar'] is not of type 'string'")
        self.assertEqual(syntax_error.spec_path, "tasks.task1.next[0].when")
        self.assertIn("properties.tasks.patternProperties", syntax_error.schema_path)

        self.assertEqual(len(test_case.expected_inspection_errors.context), 1)
        context_error = test_case.expected_inspection_errors.context[0]
        self.assertEqual(context_error.type, "yaql")
        self.assertEqual(context_error.expression, '<% ctx("__xyz") %>')
        self.assertEqual(context_error.schema_path, "properties.output")
        self.assertIn('Variable "__xyz"', context_error.message)

    def test_load_test_case_dict_with_expected_errors(self):
        test_case_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2"],
            "mock_action_executions": [{"task_id": "task2", "status": statuses.FAILED}],
            "expected_errors": [
                {"type": "error", "message": "Unknown exception at task2."},
                {"type": "error", "message": "Stuff happens.", "task_id": "task2"},
            ],
        }

        test_case = rehearsing.load_test_case(test_case_spec)

        self.assertEqual(len(test_case.mock_action_executions), 1)
        self.assertEqual(test_case.mock_action_executions[0].task_id, "task2")

        self.assertEqual(len(test_case.expected_errors), 2)
        self.assertEqual(test_case.expected_errors[0].type, "error")
        self.assertEqual(test_case.expected_errors[0].message, "Unknown exception at task2.")
        self.assertIsNone(test_case.expected_errors[0].task_id)
        self.assertIsNone(test_case.expected_errors[0].route)
        self.assertIsNone(test_case.expected_errors[0].task_transition_id)
        self.assertIsNone(test_case.expected_errors[0].result)
        self.assertIsNone(test_case.expected_errors[0].data)
        self.assertEqual(test_case.expected_errors[1].type, "error")
        self.assertEqual(test_case.expected_errors[1].message, "Stuff happens.")
        self.assertEqual(test_case.expected_errors[1].task_id, "task2")
        self.assertIsNone(test_case.expected_errors[1].route)
        self.assertIsNone(test_case.expected_errors[1].task_transition_id)
        self.assertIsNone(test_case.expected_errors[1].result)
        self.assertIsNone(test_case.expected_errors[1].data)

    def test_load_rerun_test_case_dict_minimal(self):
        workflow_state = {
            "spec": {
                "catalog": "native",
                "version": "1.0",
                "spec": {
                    "version": 1.0,
                    "description": "A basic sequential workflow.",
                    "input": ["name"],
                    "vars": [{"greeting": None}],
                    "output": [{"greeting": "<% ctx().greeting %>"}],
                    "tasks": {
                        "task1": {
                            "action": "core.echo message=<% ctx().name %>",
                            "next": [
                                {
                                    "when": "<% succeeded() %>",
                                    "publish": "greeting=<% result() %>",
                                    "do": "task2",
                                }
                            ],
                        },
                        "task2": {
                            "action": "core.echo",
                            "input": {"message": "All your base are belong to us!"},
                            "next": [
                                {
                                    "when": "<% succeeded() %>",
                                    "publish": [
                                        {"greeting": '<% ctx("greeting") %>, <% result() %>'}
                                    ],
                                    "do": ["task3"],
                                }
                            ],
                        },
                        "task3": {
                            "action": "core.echo message=<% ctx('greeting') %>",
                            "next": [
                                {"when": "<% succeeded() %>", "publish": "greeting=<% result() %>"}
                            ],
                        },
                    },
                },
            },
            "graph": {
                "directed": True,
                "multigraph": True,
                "graph": [],
                "nodes": [{"id": "task1"}, {"id": "task2"}, {"id": "task3"}, {"id": "continue"}],
                "adjacency": [
                    [{"criteria": ["<% succeeded() %>"], "ref": 0, "id": "task2", "key": 0}],
                    [{"criteria": ["<% succeeded() %>"], "ref": 0, "id": "task3", "key": 0}],
                    [{"criteria": ["<% succeeded() %>"], "ref": 0, "id": "continue", "key": 0}],
                    [],
                ],
            },
            "input": {"name": "Stanley"},
            "context": {},
            "state": {
                "contexts": [{"name": "Stanley", "greeting": None}, {"greeting": "Stanley"}],
                "routes": [[]],
                "sequence": [
                    {
                        "id": "task1",
                        "route": 0,
                        "ctxs": {"in": [0], "out": {"task2__t0": 1}},
                        "prev": {},
                        "next": {"task2__t0": True},
                        "status": "succeeded",
                    },
                    {
                        "id": "task2",
                        "route": 0,
                        "ctxs": {"in": [0, 1]},
                        "prev": {"task1__t0": 0},
                        "next": {"task3__t0": False},
                        "status": "failed",
                        "term": True,
                    },
                ],
                "staged": [],
                "status": "failed",
                "tasks": {"task1__r0": 0, "task2__r0": 1},
            },
            "log": [],
            "errors": [
                {
                    "type": "error",
                    "message": "Execution failed. See result for details.",
                    "task_id": "task2",
                }
            ],
            "output": {"greeting": "Stanley"},
        }

        test_case_spec = {
            "workflow_state": workflow_state,
            "rerun_tasks": [{"task_id": "task2"}],
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
        }

        test_case = rehearsing.load_test_case(test_case_spec)

        self.assertIsInstance(test_case, rehearsing.WorkflowRerunTestCase)
        self.assertIsInstance(test_case.conductor, conducting.WorkflowConductor)
        self.assertEqual(len(test_case.rerun_tasks), 1)
        self.assertEqual(test_case.rerun_tasks[0].task_id, "task2")
        self.assertEqual(test_case.rerun_tasks[0].route, 0)
        self.assertEqual(test_case.rerun_tasks[0].reset_items, False)

        self.assertListEqual(
            test_case.expected_task_sequence, test_case_spec["expected_task_sequence"]
        )

        self.assertDictEqual(test_case.expected_inspection_errors, {})
        self.assertListEqual(test_case.expected_routes, [[]])
        self.assertListEqual(test_case.mock_action_executions, [])
        self.assertEqual(test_case.expected_workflow_status, statuses.SUCCEEDED)
        self.assertIsNone(test_case.expected_term_tasks)
        self.assertIsNone(test_case.expected_errors)
        self.assertIsNone(test_case.expected_output)
