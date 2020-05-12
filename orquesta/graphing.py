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

import abc
import logging

import networkx as nx
from networkx.readwrite import json_graph

import six

from orquesta import exceptions as exc
from orquesta.utils import dictionary as dict_util
from orquesta.utils import jsonify as json_util


LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class WorkflowGraph(object):
    def __init__(self, graph=None):
        # self._graph is the graph model for the workflow. The tracking of workflow and task
        # progress and state is separate from the graph model. There are use cases where tasks
        # may be cycled and states overwritten.
        self._graph = graph if graph else nx.MultiDiGraph()

    def serialize(self):
        data = json_graph.adjacency_data(self._graph)

        data["adjacency"] = [
            sorted(outbounds, key=lambda x: x["id"]) for outbounds in data["adjacency"]
        ]

        return data

    @classmethod
    def deserialize(cls, data):
        g = json_graph.adjacency_graph(json_util.deepcopy(data), directed=True, multigraph=True)
        return cls(graph=g)

    @staticmethod
    def get_root_nodes(graph):
        nodes = [
            {"id": n, "name": graph.node[n].get("name", n)}
            for n, d in graph.in_degree().items()
            if d == 0
        ]

        return sorted(nodes, key=lambda x: x["id"])

    @property
    def roots(self):
        return self.get_root_nodes(self._graph)

    @property
    def leaves(self):
        # Reverse the graph using a copy to identify the root nodes.
        return self.get_root_nodes(self._graph.reverse(copy=True))

    def has_tasks(self):
        return len(self._graph) > 0

    def has_task(self, task_id):
        return self._graph.has_node(task_id)

    def get_task(self, task_id):
        if not self.has_task(task_id):
            raise exc.InvalidTask(task_id)

        task = {"id": task_id}
        task.update(json_util.deepcopy(self._graph.node[task_id]))

        return task

    def get_task_attributes(self, attribute):
        return dict_util.merge_dicts(
            {n: None for n in self._graph.nodes()},
            nx.get_node_attributes(self._graph, attribute),
            overwrite=True,
        )

    def add_task(self, task_id, **kwargs):
        if not self.has_task(task_id):
            self._graph.add_node(task_id, **kwargs)
        else:
            self.update_task(task_id, **kwargs)

    def update_task(self, task_id, **kwargs):
        if not self.has_task(task_id):
            raise exc.InvalidTask(task_id)

        for key, value in six.iteritems(kwargs):
            self._graph.node[task_id][key] = value

    def has_transition(self, source, destination, **kwargs):
        edges = filter(
            lambda e: e[0] == source and e[1] == destination,
            self._graph.edges(data=True, keys=True),  # pylint: disable=unexpected-keyword-arg
        )

        for attr, value in six.iteritems(kwargs):
            edges = filter(lambda e: e[3].get(attr, None) == value, list(edges))

        return list(edges)

    def get_transition(self, source, destination, key=None, **kwargs):
        if key is not None:
            edges = filter(
                lambda e: e[0] == source and e[1] == destination and e[2] == key,
                self._graph.edges(data=True, keys=True),  # pylint: disable=unexpected-keyword-arg
            )
        else:
            edges = filter(
                lambda e: e[0] == source and e[1] == destination,
                self._graph.edges(data=True, keys=True),  # pylint: disable=unexpected-keyword-arg
            )

            for attr, value in six.iteritems(kwargs):
                edges = filter(lambda e: e[3].get(attr, None) == value, list(edges))

        edges = list(edges)

        if len(edges) <= 0:
            raise exc.InvalidTaskTransition(source, destination)

        if len(edges) > 1:
            raise exc.AmbiguousTaskTransition(source, destination)

        return edges[0]

    def get_transition_attributes(self, attribute):
        return nx.get_edge_attributes(self._graph, attribute)

    def add_transition(self, source, destination, **kwargs):
        if not self.has_task(source):
            self.add_task(source)

        if not self.has_task(destination):
            self.add_task(destination)

        # Add attributes only if value is not None.
        attrs = {}

        for attr, value in six.iteritems(kwargs):
            if attr is not None:
                attrs[attr] = value

        self._graph.add_edge(source, destination, **attrs)

    def update_transition(self, source, destination, key, **kwargs):
        seq = self.get_transition(source, destination, key=key)

        for attr, value in six.iteritems(kwargs):
            self._graph[source][destination][seq[2]][attr] = value

    def get_next_transitions(self, task_id):
        return sorted(
            [e for e in self._graph.out_edges([task_id], data=True, keys=True)], key=lambda x: x[1]
        )

    def get_prev_transitions(self, task_id):
        return sorted(
            [e for e in self._graph.in_edges([task_id], data=True, keys=True)], key=lambda x: x[1]
        )

    def get_barriers(self):
        return {
            x[0]: x[1] for x in filter(lambda x: x[1].get("barrier"), self._graph.nodes(data=True))
        }

    def set_barrier(self, task_id, value="*"):
        self.update_task(task_id, barrier=value)

    def get_barrier(self, task_id):
        return self.get_task(task_id).get("barrier")

    def has_barrier(self, task_id):
        b = self.get_barrier(task_id)

        return b is not None and b != ""

    def get_cycles(self):
        return [
            {"tasks": sorted(c), "route": nx.find_cycle(self._graph, c)}
            for c in nx.simple_cycles(self._graph)
        ]

    def in_cycle(self, task_id):
        return [c for c in nx.simple_cycles(self._graph) if task_id in c]

    def is_cycle_closed(self, cycle):
        # A cycle is closed, for a lack of better term, if there is no task
        # transition to any task that is not a member of the cycle.
        for task_id in cycle["tasks"]:
            for transition in self.get_next_transitions(task_id):
                if transition[1] not in cycle["tasks"]:
                    return False

        return True

    def get_task_retry_spec(self, task_id):
        return self.get_task(task_id).get("retry")

    def task_has_retry(self, task_id):
        r = self.get_task_retry_spec(task_id)

        return r is not None and isinstance(r, dict) and "count" in r
