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


class SplitWorkflowComposerTest(base.OrchestraWorkflowComposerTest):
    def test_split(self):
        wf_name = "split"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1"},
                {"id": "task2"},
                {"id": "task3"},
                {"id": "task4", "splits": ["task4"]},
                {"id": "task5", "splits": ["task4"]},
                {"id": "task6", "splits": ["task4"]},
                {"id": "task7", "barrier": "*", "splits": ["task4"]},
            ],
            "adjacency": [
                [
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task3", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "task5", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task6", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
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
                {"id": "task4", "splits": ["task4"]},
                {"id": "task5", "splits": ["task4"]},
                {"id": "task6", "splits": ["task4"]},
                {"id": "task7", "barrier": "*", "splits": ["task4"]},
            ],
            "adjacency": [
                [
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task3", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "task5", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task6", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_splits(self):
        wf_name = "splits"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1"},
                {"id": "task2"},
                {"id": "task3"},
                {"id": "task4", "splits": ["task4"]},
                {"id": "task5", "splits": ["task4"]},
                {"id": "task6", "splits": ["task4"]},
                {"id": "task7", "barrier": "*", "splits": ["task4"]},
                {"id": "task8", "splits": ["task4", "task8"]},
            ],
            "adjacency": [
                [
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task3", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task8", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "task5", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task6", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task8", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
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
                {"id": "task4", "splits": ["task4"]},
                {"id": "task5", "splits": ["task4"]},
                {"id": "task6", "splits": ["task4"]},
                {"id": "task7", "barrier": "*", "splits": ["task4"]},
                {"id": "task8", "splits": ["task4", "task8"]},
            ],
            "adjacency": [
                [
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task3", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task8", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "task5", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task6", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task8", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_nested_splits(self):
        wf_name = "splits-nested"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1"},
                {"id": "task2"},
                {"id": "task3"},
                {"id": "task4", "splits": ["task4"]},
                {"id": "task5", "splits": ["task4"]},
                {"id": "task6", "splits": ["task4"]},
                {"id": "task7", "splits": ["task4", "task7"]},
                {"id": "task8", "splits": ["task4", "task7"]},
                {"id": "task9", "splits": ["task4", "task7"]},
                {"id": "task10", "barrier": "*", "splits": ["task4", "task7"]},
            ],
            "adjacency": [
                [
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task3", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "task5", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task6", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "task8", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task9", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task10", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task10", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
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
                {"id": "task4", "splits": ["task4"]},
                {"id": "task5", "splits": ["task4"]},
                {"id": "task6", "splits": ["task4"]},
                {"id": "task7", "splits": ["task4", "task7"]},
                {"id": "task8", "splits": ["task4", "task7"]},
                {"id": "task9", "splits": ["task4", "task7"]},
                {"id": "task10", "barrier": "*", "splits": ["task4", "task7"]},
            ],
            "adjacency": [
                [
                    {"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task3", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task4", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "task5", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task6", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task7", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"id": "task8", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                    {"id": "task9", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]},
                ],
                [{"id": "task10", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [{"id": "task10", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_very_many_splits(self):
        wf_name = "splits-very-many"

        expected_wf_graph = {
            "directed": True,
            "graph": [],
            "nodes": [
                {"id": "task1"},
                {"splits": ["task2"], "id": "task2"},
                {"splits": ["task2"], "id": "task3"},
                {"splits": ["task2"], "id": "task4"},
                {"splits": ["task2", "task5"], "id": "task5"},
                {"splits": ["task2", "task5"], "id": "task6"},
                {"splits": ["task2", "task5"], "id": "task7"},
                {"splits": ["task2", "task5", "task8"], "id": "task8"},
                {"splits": ["task2", "task5", "task8", "task9"], "id": "task9"},
                {"splits": ["task2", "task14", "task17"], "id": "task18"},
                {"id": "init"},
                {"splits": ["task2", "task14", "task17"], "id": "task19"},
                {
                    "splits": [
                        "task2",
                        "task5",
                        "task8",
                        "task9",
                        "task11",
                        "task14",
                        "task17",
                        "notify",
                    ],
                    "id": "notify",
                },
                {"splits": ["task2", "task5", "task8", "task9", "task11"], "id": "task12"},
                {"splits": ["task2", "task5", "task8", "task9", "task11"], "id": "task13"},
                {"splits": ["task2", "task5", "task8", "task9"], "id": "task10"},
                {"splits": ["task2", "task5", "task8", "task9", "task11"], "id": "task11"},
                {"splits": ["task2", "task14"], "id": "task16"},
                {
                    "splits": ["task2", "task5", "task8", "task9", "task11", "task14", "task17"],
                    "id": "task17",
                },
                {
                    "splits": ["task2", "task5", "task8", "task9", "task11", "task14"],
                    "id": "task14",
                },
                {
                    "splits": ["task2", "task5", "task8", "task9", "task11", "task14"],
                    "id": "task15",
                },
            ],
            "adjacency": [
                [{"ref": 0, "id": "task2", "key": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"ref": 2, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                    {
                        "ref": 0,
                        "id": "task3",
                        "key": 0,
                        "criteria": ["<% succeeded() and ctx().fork2 %>"],
                    },
                    {
                        "ref": 1,
                        "id": "task5",
                        "key": 0,
                        "criteria": ["<% succeeded() and not ctx().fork2 %>"],
                    },
                ],
                [{"ref": 0, "id": "task4", "key": 0, "criteria": ["<% succeeded() %>"]}],
                [{"ref": 0, "id": "task5", "key": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {
                        "ref": 0,
                        "id": "task6",
                        "key": 0,
                        "criteria": ["<% succeeded() and ctx().fork3 %>"],
                    },
                    {
                        "ref": 1,
                        "id": "task7",
                        "key": 0,
                        "criteria": ["<% succeeded() and not ctx().fork3 %>"],
                    },
                ],
                [
                    {"ref": 2, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                    {
                        "ref": 0,
                        "id": "task8",
                        "key": 0,
                        "criteria": ["<% succeeded() and ctx().fork4 %>"],
                    },
                    {
                        "ref": 1,
                        "id": "task9",
                        "key": 0,
                        "criteria": ["<% succeeded() and not ctx().fork4 %>"],
                    },
                ],
                [
                    {"ref": 2, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                    {
                        "ref": 0,
                        "id": "task8",
                        "key": 0,
                        "criteria": ["<% succeeded() and ctx().fork4 %>"],
                    },
                    {
                        "ref": 1,
                        "id": "task9",
                        "key": 0,
                        "criteria": ["<% succeeded() and not ctx().fork4 %>"],
                    },
                ],
                [{"ref": 0, "id": "task9", "key": 0, "criteria": []}],
                [
                    {"ref": 0, "id": "task10", "key": 0, "criteria": ["<% not ctx().fork5 %>"]},
                    {"ref": 1, "id": "task11", "key": 0, "criteria": ["<% ctx().fork5 %>"]},
                ],
                [
                    {
                        "ref": 1,
                        "id": "task18",
                        "key": 0,
                        "criteria": ["<% succeeded() and not result() %>"],
                    },
                    {
                        "ref": 0,
                        "id": "task19",
                        "key": 0,
                        "criteria": ["<% succeeded() and result() %>"],
                    },
                ],
                [
                    {"ref": 0, "id": "notify", "key": 0, "criteria": ["<% ctx().fork1 %>"]},
                    {"ref": 1, "id": "notify", "key": 1, "criteria": ["<% not ctx().fork1 %>"]},
                    {"ref": 0, "id": "task1", "key": 0, "criteria": ["<% ctx().fork1 %>"]},
                    {"ref": 1, "id": "task2", "key": 0, "criteria": ["<% not ctx().fork1 %>"]},
                ],
                [{"ref": 0, "id": "notify", "key": 0, "criteria": []}],
                [],
                [
                    {"ref": 0, "id": "task13", "key": 0, "criteria": ["<% succeeded() %>"]},
                    {"ref": 1, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                ],
                [
                    {"ref": 0, "id": "task14", "key": 0, "criteria": ["<% succeeded() %>"]},
                    {"ref": 1, "id": "task14", "key": 1, "criteria": ["<% failed()  %>"]},
                ],
                [{"ref": 0, "id": "task11", "key": 0, "criteria": []}],
                [
                    {"ref": 0, "id": "task12", "key": 0, "criteria": ["<% succeeded() %>"]},
                    {"ref": 1, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                ],
                [
                    {
                        "ref": 1,
                        "id": "task16",
                        "key": 0,
                        "criteria": ["<% succeeded() and not result() %>"],
                    },
                    {
                        "ref": 0,
                        "id": "task17",
                        "key": 0,
                        "criteria": ["<% succeeded() and result() %>"],
                    },
                ],
                [
                    {"ref": 1, "id": "notify", "key": 0, "criteria": ["<% not ctx().fork8 %>"]},
                    {"ref": 0, "id": "task18", "key": 0, "criteria": ["<% ctx().fork8 %>"]},
                ],
                [
                    {"ref": 0, "id": "task15", "key": 0, "criteria": ["<% not ctx().fork6 %>"]},
                    {"ref": 1, "id": "task16", "key": 0, "criteria": ["<% ctx().fork6 %>"]},
                ],
                [
                    {"ref": 0, "id": "task16", "key": 0, "criteria": ["<% ctx().fork7 %>"]},
                    {"ref": 1, "id": "task17", "key": 0, "criteria": ["<% not ctx().fork7 %>"]},
                ],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": [],
            "nodes": [
                {"id": "task1"},
                {"splits": ["task2"], "id": "task2"},
                {"splits": ["task2"], "id": "task3"},
                {"splits": ["task2"], "id": "task4"},
                {"splits": ["task2", "task5"], "id": "task5"},
                {"splits": ["task2", "task5"], "id": "task6"},
                {"splits": ["task2", "task5"], "id": "task7"},
                {"splits": ["task2", "task5", "task8"], "id": "task8"},
                {"splits": ["task2", "task5", "task8", "task9"], "id": "task9"},
                {"splits": ["task2", "task14", "task17"], "id": "task18"},
                {"id": "init"},
                {"splits": ["task2", "task14", "task17"], "id": "task19"},
                {
                    "splits": [
                        "task2",
                        "task5",
                        "task8",
                        "task9",
                        "task11",
                        "task14",
                        "task17",
                        "notify",
                    ],
                    "id": "notify",
                },
                {"splits": ["task2", "task5", "task8", "task9", "task11"], "id": "task12"},
                {"splits": ["task2", "task5", "task8", "task9", "task11"], "id": "task13"},
                {"splits": ["task2", "task5", "task8", "task9"], "id": "task10"},
                {"splits": ["task2", "task5", "task8", "task9", "task11"], "id": "task11"},
                {"splits": ["task2", "task14"], "id": "task16"},
                {
                    "splits": ["task2", "task5", "task8", "task9", "task11", "task14", "task17"],
                    "id": "task17",
                },
                {
                    "splits": ["task2", "task5", "task8", "task9", "task11", "task14"],
                    "id": "task14",
                },
                {
                    "splits": ["task2", "task5", "task8", "task9", "task11", "task14"],
                    "id": "task15",
                },
            ],
            "adjacency": [
                [{"ref": 0, "id": "task2", "key": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {"ref": 2, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                    {
                        "ref": 0,
                        "id": "task3",
                        "key": 0,
                        "criteria": ["<% succeeded() and ctx().fork2 %>"],
                    },
                    {
                        "ref": 1,
                        "id": "task5",
                        "key": 0,
                        "criteria": ["<% succeeded() and not ctx().fork2 %>"],
                    },
                ],
                [{"ref": 0, "id": "task4", "key": 0, "criteria": ["<% succeeded() %>"]}],
                [{"ref": 0, "id": "task5", "key": 0, "criteria": ["<% succeeded() %>"]}],
                [
                    {
                        "ref": 0,
                        "id": "task6",
                        "key": 0,
                        "criteria": ["<% succeeded() and ctx().fork3 %>"],
                    },
                    {
                        "ref": 1,
                        "id": "task7",
                        "key": 0,
                        "criteria": ["<% succeeded() and not ctx().fork3 %>"],
                    },
                ],
                [
                    {"ref": 2, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                    {
                        "ref": 0,
                        "id": "task8",
                        "key": 0,
                        "criteria": ["<% succeeded() and ctx().fork4 %>"],
                    },
                    {
                        "ref": 1,
                        "id": "task9",
                        "key": 0,
                        "criteria": ["<% succeeded() and not ctx().fork4 %>"],
                    },
                ],
                [
                    {"ref": 2, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                    {
                        "ref": 0,
                        "id": "task8",
                        "key": 0,
                        "criteria": ["<% succeeded() and ctx().fork4 %>"],
                    },
                    {
                        "ref": 1,
                        "id": "task9",
                        "key": 0,
                        "criteria": ["<% succeeded() and not ctx().fork4 %>"],
                    },
                ],
                [{"ref": 0, "id": "task9", "key": 0, "criteria": []}],
                [
                    {"ref": 0, "id": "task10", "key": 0, "criteria": ["<% not ctx().fork5 %>"]},
                    {"ref": 1, "id": "task11", "key": 0, "criteria": ["<% ctx().fork5 %>"]},
                ],
                [
                    {
                        "ref": 1,
                        "id": "task18",
                        "key": 0,
                        "criteria": ["<% succeeded() and not result() %>"],
                    },
                    {
                        "ref": 0,
                        "id": "task19",
                        "key": 0,
                        "criteria": ["<% succeeded() and result() %>"],
                    },
                ],
                [
                    {"ref": 0, "id": "notify", "key": 0, "criteria": ["<% ctx().fork1 %>"]},
                    {"ref": 1, "id": "notify", "key": 1, "criteria": ["<% not ctx().fork1 %>"]},
                    {"ref": 0, "id": "task1", "key": 0, "criteria": ["<% ctx().fork1 %>"]},
                    {"ref": 1, "id": "task2", "key": 0, "criteria": ["<% not ctx().fork1 %>"]},
                ],
                [{"ref": 0, "id": "notify", "key": 0, "criteria": []}],
                [],
                [
                    {"ref": 0, "id": "task13", "key": 0, "criteria": ["<% succeeded() %>"]},
                    {"ref": 1, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                ],
                [
                    {"ref": 0, "id": "task14", "key": 0, "criteria": ["<% succeeded() %>"]},
                    {"ref": 1, "id": "task14", "key": 1, "criteria": ["<% failed()  %>"]},
                ],
                [{"ref": 0, "id": "task11", "key": 0, "criteria": []}],
                [
                    {"ref": 0, "id": "task12", "key": 0, "criteria": ["<% succeeded() %>"]},
                    {"ref": 1, "id": "task14", "key": 0, "criteria": ["<% failed() %>"]},
                ],
                [
                    {
                        "ref": 1,
                        "id": "task16",
                        "key": 0,
                        "criteria": ["<% succeeded() and not result() %>"],
                    },
                    {
                        "ref": 0,
                        "id": "task17",
                        "key": 0,
                        "criteria": ["<% succeeded() and result() %>"],
                    },
                ],
                [
                    {"ref": 1, "id": "notify", "key": 0, "criteria": ["<% not ctx().fork8 %>"]},
                    {"ref": 0, "id": "task18", "key": 0, "criteria": ["<% ctx().fork8 %>"]},
                ],
                [
                    {"ref": 0, "id": "task15", "key": 0, "criteria": ["<% not ctx().fork6 %>"]},
                    {"ref": 1, "id": "task16", "key": 0, "criteria": ["<% ctx().fork6 %>"]},
                ],
                [
                    {"ref": 0, "id": "task16", "key": 0, "criteria": ["<% ctx().fork7 %>"]},
                    {"ref": 1, "id": "task17", "key": 0, "criteria": ["<% not ctx().fork7 %>"]},
                ],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)
