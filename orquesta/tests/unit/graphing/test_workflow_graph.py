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

from orquesta import exceptions as exc
from orquesta import graphing
from orquesta.tests.unit import base as test_base
from orquesta.utils import jsonify as json_util


EXPECTED_WF_GRAPH = {
    "directed": True,
    "graph": [],
    "nodes": [
        {"id": "task1"},
        {"id": "task2"},
        {"id": "task3"},
        {"id": "task4"},
        {"id": "task5", "barrier": "*"},
        {"id": "task6"},
        {"id": "task7"},
        {"id": "task8"},
        {"id": "task9"},
    ],
    "adjacency": [
        [
            {"id": "task2", "key": 0, "attr1": "foobar"},
            {"id": "task4", "key": 0},
            {"id": "task7", "key": 0},
            {"id": "task9", "key": 0},
        ],
        [{"id": "task3", "key": 0}],
        [{"id": "task5", "key": 0}],
        [{"id": "task5", "key": 0}],
        [{"id": "task6", "key": 0}],
        [],
        [{"id": "task8", "key": 0}],
        [{"id": "task9", "key": 0}],
        [],
    ],
    "multigraph": True,
}


class WorkflowGraphTest(test_base.WorkflowGraphTest):
    def _add_tasks(self, wf_graph):
        for i in range(1, 10):
            wf_graph.add_task("task" + str(i))

    def _add_transitions(self, wf_graph):
        wf_graph.add_transition("task1", "task2", attr1="foobar")
        wf_graph.add_transition("task2", "task3")
        wf_graph.add_transition("task1", "task4")
        wf_graph.add_transition("task3", "task5")
        wf_graph.add_transition("task4", "task5")
        wf_graph.add_transition("task5", "task6")
        wf_graph.add_transition("task1", "task7")
        wf_graph.add_transition("task7", "task8")
        wf_graph.add_transition("task1", "task9")
        wf_graph.add_transition("task8", "task9")

    def _add_barriers(self, wf_graph):
        wf_graph.update_task("task5", barrier="*")

    def _prep_graph(self):
        wf_graph = graphing.WorkflowGraph()

        self._add_tasks(wf_graph)
        self._add_transitions(wf_graph)
        self._add_barriers(wf_graph)

        return wf_graph

    def test_graph(self):
        wf_graph = self._prep_graph()

        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_graph_roots(self):
        wf_graph = self._prep_graph()

        expected = [{"id": "task1", "name": "task1"}]

        self.assertListEqual(wf_graph.roots, expected)

    def test_graph_leaves(self):
        wf_graph = self._prep_graph()

        expected_leaves = [{"id": "task6", "name": "task6"}, {"id": "task9", "name": "task9"}]

        self.assertListEqual(wf_graph.leaves, expected_leaves)

        # Ensure the underlying graph is not permanently altered.
        expected_roots = [{"id": "task1", "name": "task1"}]

        self.assertListEqual(wf_graph.roots, expected_roots)

    def test_skip_add_tasks(self):
        wf_graph = graphing.WorkflowGraph()

        self._add_transitions(wf_graph)
        self._add_barriers(wf_graph)

        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_duplicate_add_tasks(self):
        wf_graph = self._prep_graph()

        self._add_tasks(wf_graph)

        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_add_same_transition_between_tasks(self):
        wf_graph = self._prep_graph()

        wf_graph.add_transition("task1", "task2", key=0, attr1="foobar")

        self.assert_graph_equal(wf_graph, EXPECTED_WF_GRAPH)

    def test_add_duplicate_transition_between_tasks(self):
        wf_graph = self._prep_graph()

        wf_graph.add_transition("task1", "task2", attr1="fubar")

        expected_wf_graph = json_util.deepcopy(EXPECTED_WF_GRAPH)
        expected_transition = {"id": "task2", "key": 1, "attr1": "fubar"}
        expected_wf_graph["adjacency"][0].append(expected_transition)

        self.assert_graph_equal(wf_graph, expected_wf_graph)

    def test_get_task(self):
        wf_graph = self._prep_graph()

        expected = {"id": "task1"}
        self.assertDictEqual(wf_graph.get_task("task1"), expected)

    def test_get_nonexistent_task(self):
        wf_graph = self._prep_graph()

        self.assertRaises(exc.InvalidTask, wf_graph.get_task, "task999")

    def test_update_task(self):
        wf_graph = self._prep_graph()

        expected = {"id": "task1"}
        self.assertDictEqual(wf_graph.get_task("task1"), expected)

        # Update task with new attributes.
        wf_graph.update_task("task1", attr1="foobar", attr2="fubar")
        expected = {"id": "task1", "attr1": "foobar", "attr2": "fubar"}
        self.assertDictEqual(wf_graph.get_task("task1"), expected)

        # Update an existing attribute.
        wf_graph.update_task("task1", attr2="foobar")
        expected = {"id": "task1", "attr1": "foobar", "attr2": "foobar"}
        self.assertDictEqual(wf_graph.get_task("task1"), expected)

    def test_update_nonexistent_task(self):
        wf_graph = self._prep_graph()

        self.assertRaises(exc.InvalidTask, wf_graph.update_task, "task999", attr1="foobar")

    def test_get_task_attributes(self):
        wf_graph = self._prep_graph()

        wf_graph.update_task("task1", attr1="foobar")
        wf_graph.update_task("task2", attr1="foobar")
        wf_graph.update_task("task4", attr1="foobar")

        expected = {
            "task1": "foobar",
            "task2": "foobar",
            "task3": None,
            "task4": "foobar",
            "task5": None,
            "task6": None,
            "task7": None,
            "task8": None,
            "task9": None,
        }

        self.assertDictEqual(wf_graph.get_task_attributes("attr1"), expected)

    def test_has_transition(self):
        wf_graph = self._prep_graph()

        expected = [("task1", "task2", 0, {"attr1": "foobar"})]
        self.assertListEqual(wf_graph.has_transition("task1", "task2", attr1="foobar"), expected)

        expected = [("task2", "task3", 0, {})]
        self.assertListEqual(wf_graph.has_transition("task2", "task3"), expected)

    def test_get_transition(self):
        wf_graph = self._prep_graph()

        expected = ("task1", "task2", 0, {"attr1": "foobar"})
        self.assertEqual(wf_graph.get_transition("task1", "task2", attr1="foobar"), expected)

        expected = ("task2", "task3", 0, {})
        self.assertEqual(wf_graph.get_transition("task2", "task3"), expected)

    def test_get_nonexistent_transition(self):
        wf_graph = self._prep_graph()

        self.assertRaises(
            exc.InvalidTaskTransition, wf_graph.get_transition, "task1", "task2", attr1="fubar"
        )

        self.assertRaises(exc.InvalidTaskTransition, wf_graph.get_transition, "task998", "task999")

    def test_get_ambiguous_transition(self):
        wf_graph = graphing.WorkflowGraph()

        self._add_tasks(wf_graph)

        wf_graph._graph.add_edge("task1", "task2")
        wf_graph._graph.add_edge("task1", "task2")

        self.assertRaises(exc.AmbiguousTaskTransition, wf_graph.get_transition, "task1", "task2")

    def test_get_next_transitions(self):
        wf_graph = self._prep_graph()

        expected_transitions = [
            ("task1", "task2", 0, {"attr1": "foobar"}),
            ("task1", "task4", 0, {}),
            ("task1", "task7", 0, {}),
            ("task1", "task9", 0, {}),
        ]

        self.assertListEqual(
            sorted(wf_graph.get_next_transitions("task1")), sorted(expected_transitions)
        )

    def test_get_prev_transitions(self):
        wf_graph = self._prep_graph()

        expected_transitions = [("task3", "task5", 0, {}), ("task4", "task5", 0, {})]

        self.assertListEqual(
            sorted(wf_graph.get_prev_transitions("task5")), sorted(expected_transitions)
        )

    def test_task_has_barrier(self):
        wf_graph = self._prep_graph()

        self.assertTrue(wf_graph.has_barrier("task5"))
        self.assertFalse(wf_graph.has_barrier("task9"))

    def test_get_barrier_tasks(self):
        wf_graph = self._prep_graph()

        expected_result = {"task5": {"barrier": "*"}}
        self.assertDictEqual(wf_graph.get_barriers(), expected_result)

    def test_split_from_reused_task(self):
        wf_graph = self._prep_graph()

        self.assertFalse(
            len(wf_graph.get_prev_transitions("task5")) > 1 and not wf_graph.has_barrier("task5")
        )

        self.assertTrue(
            len(wf_graph.get_prev_transitions("task9")) > 1 and not wf_graph.has_barrier("task9")
        )
