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

from orquesta.tests.unit.composition.native import base


class ErrorHandlingWorkflowComposerTest(base.OrchestraWorkflowComposerTest):
    def test_error_handling_continue(self):
        wf_name = "error-handling-continue"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [{"id": "task1"}, {"id": "task2"}, {"id": "continue", "splits": ["continue"]}],
            "adjacency": [
                [
                    {"id": "continue", "key": 0, "ref": 1, "criteria": ["<% failed() %>"]},
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "continue", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": {},
            "nodes": [{"id": "task1"}, {"id": "task2"}, {"id": "continue", "splits": ["continue"]}],
            "adjacency": [
                [
                    {"id": "continue", "key": 0, "ref": 1, "criteria": ["<% failed() %>"]},
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "continue", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_error_handling_noop(self):
        wf_name = "error-handling-noop"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [{"id": "task1"}, {"id": "task2"}, {"id": "continue"}, {"id": "noop"}],
            "adjacency": [
                [
                    {"id": "noop", "key": 0, "ref": 1, "criteria": ["<% failed() %>"]},
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "continue", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": {},
            "nodes": [{"id": "task1"}, {"id": "task2"}, {"id": "continue"}, {"id": "noop"}],
            "adjacency": [
                [
                    {"id": "noop", "key": 0, "ref": 1, "criteria": ["<% failed() %>"]},
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "continue", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_error_handling_fail(self):
        wf_name = "error-handling-fail"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1"},
                {"id": "task2"},
                {"id": "task3"},
                {"id": "continue"},
                {"id": "fail"},
            ],
            "adjacency": [
                [
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task3", "key": 0, "ref": 1, "criteria": ["<% failed() %>"]},
                ],
                [{"id": "continue", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "fail", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1"},
                {"id": "task2"},
                {"id": "task3"},
                {"id": "continue"},
                {"id": "fail"},
            ],
            "adjacency": [
                [
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task3", "key": 0, "ref": 1, "criteria": ["<% failed() %>"]},
                ],
                [{"id": "continue", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "fail", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)
