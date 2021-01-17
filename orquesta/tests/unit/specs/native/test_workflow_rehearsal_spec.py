# Copyright 2021 The StackStorm Authors.
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

import os
import shutil
import six
import tempfile

from orquesta import conducting
from orquesta import exceptions as exc
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

    def test_init_test_spec(self):
        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
        }

        test_case = rehearsing.WorkflowTestCase(test_spec)

        self.assertIsInstance(test_case, rehearsing.WorkflowTestCase)
        self.assertEqual(test_case.workflow, test_spec["workflow"])

    def test_load_test_spec_yaml_minimal(self):
        test_spec = """---
        workflow: %s
        expected_task_sequence:
          - task1
          - task2
          - task3
          - continue
        """

        rehearsal = rehearsing.load_test_spec(test_spec % self.get_wf_file_path("sequential"))

        self.assertIsInstance(rehearsal.session, rehearsing.WorkflowTestCase)
        self.assertEqual(rehearsal.session.workflow, self.get_wf_file_path("sequential"))

        self.assertListEqual(
            rehearsal.session.expected_task_sequence, ["task1", "task2", "task3", "continue"]
        )

        self.assertTrue(rehearsal.session.wf_def.startswith("version"))
        self.assertDictEqual(rehearsal.session.inputs, {})
        self.assertDictEqual(rehearsal.session.expected_inspection_errors, {})
        self.assertListEqual(rehearsal.session.expected_routes, [[]])
        self.assertListEqual(rehearsal.session.mock_action_executions, [])
        self.assertEqual(rehearsal.session.expected_workflow_status, statuses.SUCCEEDED)
        self.assertIsNone(rehearsal.session.expected_term_tasks)
        self.assertIsNone(rehearsal.session.expected_errors)
        self.assertIsNone(rehearsal.session.expected_output)

    def test_load_test_spec_dict_minimal(self):
        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertIsInstance(rehearsal.session, rehearsing.WorkflowTestCase)
        self.assertEqual(rehearsal.session.workflow, test_spec["workflow"])

        self.assertListEqual(
            rehearsal.session.expected_task_sequence, test_spec["expected_task_sequence"]
        )

        self.assertTrue(rehearsal.session.wf_def.startswith("version"))
        self.assertDictEqual(rehearsal.session.inputs, {})
        self.assertDictEqual(rehearsal.session.expected_inspection_errors, {})
        self.assertListEqual(rehearsal.session.expected_routes, [[]])
        self.assertListEqual(rehearsal.session.mock_action_executions, [])
        self.assertEqual(rehearsal.session.expected_workflow_status, statuses.SUCCEEDED)
        self.assertIsNone(rehearsal.session.expected_term_tasks)
        self.assertIsNone(rehearsal.session.expected_errors)
        self.assertIsNone(rehearsal.session.expected_output)

    def test_load_test_spec_dict_with_mock_action_executions(self):
        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [{"task_id": "task1"}, {"task_id": "task2"}],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertEqual(len(rehearsal.session.mock_action_executions), 2)
        self.assertEqual(rehearsal.session.mock_action_executions[0].task_id, "task1")
        self.assertEqual(rehearsal.session.mock_action_executions[1].task_id, "task2")

        for ac_ex in rehearsal.session.mock_action_executions:
            self.assertIsInstance(ac_ex, rehearsing.MockActionExecution)
            self.assertEqual(ac_ex.route, 0)
            self.assertIsNone(ac_ex.seq_id)
            self.assertIsNone(ac_ex.item_id)
            self.assertEqual(ac_ex.iter_id, 0)
            self.assertEqual(ac_ex.num_iter, 1)
            self.assertEqual(ac_ex.status, statuses.SUCCEEDED)
            self.assertIsNone(ac_ex.result)

    def test_init_test_spec_with_mock_action_execution_text_result_path(self):
        fd, path = tempfile.mkstemp()

        with os.fdopen(fd, "w") as tmp:
            tmp.write("foobar")

        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [{"task_id": "task1", "result_path": path}],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertEqual(len(rehearsal.session.mock_action_executions), 1)
        self.assertEqual(rehearsal.session.mock_action_executions[0].task_id, "task1")
        self.assertEqual(rehearsal.session.mock_action_executions[0].result, "foobar")
        self.assertEqual(rehearsal.session.mock_action_executions[0].result_path, path)

    def test_init_test_spec_with_mock_action_execution_json_result_path(self):
        fd, path = tempfile.mkstemp(suffix=".json")

        with os.fdopen(fd, "w") as tmp:
            tmp.write('{"foo": "bar"}\n')

        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [{"task_id": "task1", "result_path": path}],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertTrue(path.lower().endswith(".json"))
        self.assertEqual(len(rehearsal.session.mock_action_executions), 1)
        self.assertEqual(rehearsal.session.mock_action_executions[0].task_id, "task1")
        self.assertDictEqual(rehearsal.session.mock_action_executions[0].result, {"foo": "bar"})
        self.assertEqual(rehearsal.session.mock_action_executions[0].result_path, path)

    def test_init_test_spec_with_mock_action_execution_yaml_result_path(self):
        fd, path = tempfile.mkstemp(suffix=".yaml")

        with os.fdopen(fd, "w") as tmp:
            tmp.write("---\n")
            tmp.write("foo: bar\n")

        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [{"task_id": "task1", "result_path": path}],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertTrue(path.lower().endswith(".yaml"))
        self.assertEqual(len(rehearsal.session.mock_action_executions), 1)
        self.assertEqual(rehearsal.session.mock_action_executions[0].task_id, "task1")
        self.assertDictEqual(rehearsal.session.mock_action_executions[0].result, {"foo": "bar"})
        self.assertEqual(rehearsal.session.mock_action_executions[0].result_path, path)

    def test_init_test_spec_with_mock_action_execution_yml_result_path(self):
        fd, path = tempfile.mkstemp(suffix=".yml")

        with os.fdopen(fd, "w") as tmp:
            tmp.write("---\n")
            tmp.write("foo: bar\n")

        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [{"task_id": "task1", "result_path": path}],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertTrue(path.lower().endswith(".yml"))
        self.assertEqual(len(rehearsal.session.mock_action_executions), 1)
        self.assertEqual(rehearsal.session.mock_action_executions[0].task_id, "task1")
        self.assertDictEqual(rehearsal.session.mock_action_executions[0].result, {"foo": "bar"})
        self.assertEqual(rehearsal.session.mock_action_executions[0].result_path, path)

    def test_init_test_spec_with_base_path(self):
        shutil.copy(self.get_wf_file_path("sequential"), "/tmp/sequential.yaml")

        fd, path = tempfile.mkstemp(suffix=".json")

        with os.fdopen(fd, "w") as tmp:
            tmp.write('{"foo": "bar"}\n')

        test_spec = {
            "workflow": "sequential.yaml",
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [
                {"task_id": "task1", "result_path": path[path.rfind("/") + 1 :]}
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec, base_path="/tmp")

        self.assertIsInstance(rehearsal.session, rehearsing.WorkflowTestCase)
        self.assertEqual(rehearsal.session.workflow, "/tmp/sequential.yaml")
        self.assertTrue(rehearsal.session.wf_def.startswith("version"))
        self.assertEqual(len(rehearsal.session.mock_action_executions), 1)
        self.assertEqual(rehearsal.session.mock_action_executions[0].task_id, "task1")
        self.assertDictEqual(rehearsal.session.mock_action_executions[0].result, {"foo": "bar"})
        self.assertEqual(rehearsal.session.mock_action_executions[0].result_path, path)

    def test_load_test_spec_dict_with_expected_inspection_errors(self):
        test_spec = {
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

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertEqual(len(rehearsal.session.expected_inspection_errors.syntax), 1)
        syntax_error = rehearsal.session.expected_inspection_errors.syntax[0]
        self.assertIsNone(syntax_error.type)
        self.assertIsNone(syntax_error.expression)
        self.assertEqual(syntax_error.message, "['foobar'] is not of type 'string'")
        self.assertEqual(syntax_error.spec_path, "tasks.task1.next[0].when")
        self.assertIn("properties.tasks.patternProperties", syntax_error.schema_path)

        self.assertEqual(len(rehearsal.session.expected_inspection_errors.context), 1)
        context_error = rehearsal.session.expected_inspection_errors.context[0]
        self.assertEqual(context_error.type, "yaql")
        self.assertEqual(context_error.expression, '<% ctx("__xyz") %>')
        self.assertEqual(context_error.schema_path, "properties.output")
        self.assertIn('Variable "__xyz"', context_error.message)

    def test_load_test_spec_dict_with_expected_errors(self):
        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2"],
            "mock_action_executions": [{"task_id": "task2", "status": statuses.FAILED}],
            "expected_errors": [
                {"type": "error", "message": "Unknown exception at task2."},
                {"type": "error", "message": "Stuff happens.", "task_id": "task2"},
            ],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertEqual(len(rehearsal.session.mock_action_executions), 1)
        self.assertEqual(rehearsal.session.mock_action_executions[0].task_id, "task2")

        self.assertEqual(len(rehearsal.session.expected_errors), 2)
        self.assertEqual(rehearsal.session.expected_errors[0].type, "error")
        self.assertEqual(
            rehearsal.session.expected_errors[0].message, "Unknown exception at task2."
        )
        self.assertIsNone(rehearsal.session.expected_errors[0].task_id)
        self.assertIsNone(rehearsal.session.expected_errors[0].route)
        self.assertIsNone(rehearsal.session.expected_errors[0].task_transition_id)
        self.assertIsNone(rehearsal.session.expected_errors[0].result)
        self.assertIsNone(rehearsal.session.expected_errors[0].data)
        self.assertEqual(rehearsal.session.expected_errors[1].type, "error")
        self.assertEqual(rehearsal.session.expected_errors[1].message, "Stuff happens.")
        self.assertEqual(rehearsal.session.expected_errors[1].task_id, "task2")
        self.assertIsNone(rehearsal.session.expected_errors[1].route)
        self.assertIsNone(rehearsal.session.expected_errors[1].task_transition_id)
        self.assertIsNone(rehearsal.session.expected_errors[1].result)
        self.assertIsNone(rehearsal.session.expected_errors[1].data)

    def test_init_rerun_test_spec(self):
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

        test_spec = {
            "workflow_state": workflow_state,
            "rerun_tasks": [{"task_id": "task2"}],
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
        }

        test_case = rehearsing.WorkflowRerunTestCase(test_spec)

        self.assertIsInstance(test_case, rehearsing.WorkflowRerunTestCase)
        self.assertIsInstance(test_case.conductor, conducting.WorkflowConductor)

    def test_load_rerun_test_spec_dict_minimal(self):
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

        test_spec = {
            "workflow_state": workflow_state,
            "rerun_tasks": [{"task_id": "task2"}],
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
        }

        rehearsal = rehearsing.load_test_spec(test_spec)

        self.assertIsInstance(rehearsal.session, rehearsing.WorkflowRerunTestCase)
        self.assertIsInstance(rehearsal.session.conductor, conducting.WorkflowConductor)
        self.assertEqual(len(rehearsal.session.rerun_tasks), 1)
        self.assertEqual(rehearsal.session.rerun_tasks[0].task_id, "task2")
        self.assertEqual(rehearsal.session.rerun_tasks[0].route, 0)
        self.assertEqual(rehearsal.session.rerun_tasks[0].reset_items, False)

        self.assertListEqual(
            rehearsal.session.expected_task_sequence, test_spec["expected_task_sequence"]
        )

        self.assertDictEqual(rehearsal.session.expected_inspection_errors, {})
        self.assertListEqual(rehearsal.session.expected_routes, [[]])
        self.assertListEqual(rehearsal.session.mock_action_executions, [])
        self.assertEqual(rehearsal.session.expected_workflow_status, statuses.SUCCEEDED)
        self.assertIsNone(rehearsal.session.expected_term_tasks)
        self.assertIsNone(rehearsal.session.expected_errors)
        self.assertIsNone(rehearsal.session.expected_output)

    def test_init_test_spec_null_type(self):
        assertRaisesRegex = self.assertRaisesRegex if six.PY3 else self.assertRaisesRegexp

        assertRaisesRegex(
            exc.WorkflowRehearsalError,
            "The session object is not provided.",
            rehearsing.WorkflowRehearsal,
            None,
        )

    def test_init_test_spec_bad_type(self):
        assertRaisesRegex = self.assertRaisesRegex if six.PY3 else self.assertRaisesRegexp

        assertRaisesRegex(
            exc.WorkflowRehearsalError,
            "The session object is not type of WorkflowTestCase or WorkflowRerunTestCase.",
            rehearsing.WorkflowRehearsal,
            object(),
        )

    def test_init_test_spec_bad_item_execution(self):
        test_spec = {
            "workflow": self.get_wf_file_path("with-items"),
            "expected_task_sequence": ["task1"],
            "mock_action_executions": [
                {"task_id": "task1", "result": "Picard, resistance is futile!"},
            ],
        }

        test_case = rehearsing.WorkflowTestCase(test_spec)

        assertRaisesRegex = self.assertRaisesRegex if six.PY3 else self.assertRaisesRegexp

        assertRaisesRegex(
            exc.WorkflowRehearsalError,
            'Mock action execution for with items task "task1" is misssing "item_id".',
            rehearsing.WorkflowRehearsal,
            test_case,
        )

    def test_init_test_spec_bad_result_path(self):
        test_spec = {
            "workflow": self.get_wf_file_path("sequential"),
            "expected_task_sequence": ["task1", "task2", "task3", "continue"],
            "mock_action_executions": [{"task_id": "task1", "result_path": "/path/does/not/exist"}],
        }

        test_case = rehearsing.WorkflowTestCase(test_spec)

        assertRaisesRegex = self.assertRaisesRegex if six.PY3 else self.assertRaisesRegexp

        assertRaisesRegex(
            exc.WorkflowRehearsalError,
            'The result path "/path/does/not/exist" for the mock action execution does not exist.',
            rehearsing.WorkflowRehearsal,
            test_case,
        )
