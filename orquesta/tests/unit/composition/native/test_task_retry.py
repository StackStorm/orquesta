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


class TaskRetryComposerTest(base.OrchestraWorkflowComposerTest):
    def test_task_with_retry_spec(self):
        wf_name = "task-retry-spec"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1", "retry": {"when": "<% failed() %>", "delay": 1, "count": 3}},
                {"id": "task2"},
            ],
            "adjacency": [
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1", "retry": {"when": "<% failed() %>", "delay": 1, "count": 3}},
                {"id": "task2"},
            ],
            "adjacency": [
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_task_with_retry_command(self):
        wf_name = "task-retry-command"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1", "retry": {"when": "<% failed() %>", "count": 3}},
                {"id": "task2"},
            ],
            "adjacency": [
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1", "retry": {"when": "<% failed() %>", "count": 3}},
                {"id": "task2"},
            ],
            "adjacency": [
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)

    def test_task_with_retry_command_default_when(self):
        wf_name = "task-retry-command-default-when"

        expected_wf_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1", "retry": {"when": "<% completed() %>", "count": 3}},
                {"id": "task2"},
            ],
            "adjacency": [
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            "directed": True,
            "graph": {},
            "nodes": [
                {"id": "task1", "retry": {"when": "<% completed() %>", "count": 3}},
                {"id": "task2"},
            ],
            "adjacency": [
                [{"id": "task2", "key": 0, "ref": 0, "criteria": ["<% succeeded() %>"]}],
                [],
            ],
            "multigraph": True,
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)
