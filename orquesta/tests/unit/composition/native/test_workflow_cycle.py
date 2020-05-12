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


class CyclicWorkflowComposerTest(base.OrchestraWorkflowComposerTest):
    def test_cycle(self):
        wf_name = "cycle"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [{"id": "prep"}, {"id": "task1"}, {"id": "task2"}, {"id": "task3"}],
            "adjacency": [
                [{"id": "task1", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task3", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {
                        "id": "task1",
                        "key": 0,
                        "ref": 0,
                        "criteria": ["<% succeeded() and ctx().count < 2 %>"],
                    }
                ],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": {},
            "nodes": [{"id": "prep"}, {"id": "task1"}, {"id": "task2"}, {"id": "task3"}],
            "adjacency": [
                [{"id": "task1", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task3", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {
                        "id": "task1",
                        "key": 0,
                        "ref": 0,
                        "criteria": ["<% succeeded() and ctx().count < 2 %>"],
                    }
                ],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_cycles(self):
        wf_name = "cycles"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "prep"},
                {"id": "task1"},
                {"id": "task2"},
                {"id": "task3"},
                {"id": "task4"},
                {"id": "task5"},
            ],
            "adjacency": [
                [{"id": "task1", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {
                        "id": "task3",
                        "key": 0,
                        "ref": 0,
                        "criteria": ["<% succeeded() and not ctx().proceed %>"],
                    },
                    {
                        "id": "task5",
                        "key": 0,
                        "ref": 1,
                        "criteria": ["<% succeeded() and ctx().proceed %>"],
                    },
                ],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {
                        "id": "task1",
                        "key": 0,
                        "ref": 0,
                        "criteria": ["<% succeeded() and ctx().count < 2 %>"],
                    }
                ],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "prep"},
                {"id": "task1"},
                {"id": "task2"},
                {"id": "task3"},
                {"id": "task4"},
                {"id": "task5"},
            ],
            "adjacency": [
                [{"id": "task1", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {
                        "id": "task3",
                        "key": 0,
                        "ref": 0,
                        "criteria": ["<% succeeded() and not ctx().proceed %>"],
                    },
                    {
                        "id": "task5",
                        "key": 0,
                        "ref": 1,
                        "criteria": ["<% succeeded() and ctx().proceed %>"],
                    },
                ],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {
                        "id": "task1",
                        "key": 0,
                        "ref": 0,
                        "criteria": ["<% succeeded() and ctx().count < 2 %>"],
                    }
                ],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_rollback_retry(self):
        wf_name = "rollback-retry"

        expected_wf_ex_graph = {
            "directed": True,
            "graph": [],
            "nodes": [
                {"id": "init"},
                {"id": "create"},
                {"id": "rollback"},
                {"id": "check"},
                {"id": "delete"},
            ],
            "adjacency": [
                [
                    {"id": "create", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "check", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [],
                [{"id": "check", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "rollback", "key": 0, "ref": 1, "criteria": ["<% failed() %>"]},
                    {"id": "delete", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)
