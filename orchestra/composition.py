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
import copy
import logging

import networkx as nx
from networkx.readwrite import json_graph
import six

from orchestra.expressions import base as expressions
from orchestra.utils import dictionary as dict_utils


LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class WorkflowGraph(object):

    def __init__(self, graph=None):
        self._graph = graph if graph else nx.MultiDiGraph()

    def serialize(self):
        return json_graph.adjacency_data(self._graph)

    @classmethod
    def deserialize(cls, data):
        g = json_graph.adjacency_graph(data, directed=True, multigraph=True)

        return cls(graph=g)

    def has_task(self, task):
        return self._graph.has_node(task)

    def get_task(self, task):
        if not self.has_task(task):
            raise Exception('Task does not exist.')

        return copy.deepcopy(self._graph.node[task])

    def get_task_attributes(self, attribute):
        return nx.get_node_attributes(self._graph, attribute)

    def add_task(self, task, **kwargs):
        if not self.has_task(task):
            self._graph.add_node(task, **kwargs)
        else:
            self.update_task(task, **kwargs)

    def update_task(self, task, **kwargs):
        if not self.has_task(task):
            raise Exception('Task does not exist.')

        for key, value in six.iteritems(kwargs):
            self._graph.node[task][key] = value

    def get_start_tasks(self):
        tasks = [
            {'id': n, 'name': self._graph.node[n].get('name', n)}
            for n, d in self._graph.in_degree().items() if d == 0
        ]

        return sorted(tasks, key=lambda x: x['name'])

    def get_next_tasks(self, task, context=None):
        self.update_task(task['id'], state=task['state'])

        context = dict_utils.merge_dicts(
            context or {},
            {'__task_states': self.get_task_attributes('state')},
            overwrite=True
        )

        tasks = []
        outbounds = []

        for seq in self.get_next_sequences(task['id']):
            evaluated_criteria = [
                expressions.evaluate(criterion, context)
                for criterion in seq[3]['criteria']
            ]

            if all(evaluated_criteria):
                outbounds.append(seq)

        for seq in outbounds:
            next_task_id, seq_key, attrs = seq[1], seq[2], seq[3]
            next_task = self.get_task(next_task_id)

            if not attrs.get('satisfied', False):
                self.update_sequence(
                    task['id'],
                    next_task_id,
                    key=seq_key,
                    satisfied=True
                )

            join_spec = next_task.get('join')

            if join_spec:
                inbounds = self.get_prev_sequences(next_task_id)
                satisfied = [s for s in inbounds if s[3].get('satisfied')]
                join_spec = len(inbounds) if join_spec == 'all' else join_spec

                if len(satisfied) < join_spec:
                    continue

            tasks.append({'id': next_task_id, 'name': next_task['name']})

        return sorted(tasks, key=lambda x: x['name'])

    def has_sequence(self, source, destination, criteria=None):
        return [
            edge for edge in self._graph.edges(data=True, keys=True)
            if (edge[0] == source and edge[1] == destination and
                edge[3].get('criteria', None) == criteria)
        ]

    def get_sequence(self, source, destination, key=None, criteria=None):
        seqs = [
            edge for edge in self._graph.edges(data=True, keys=True)
            if (edge[0] == source and edge[1] == destination and (
                edge[3].get('criteria', None) == criteria or
                edge[2] == key))
        ]

        if not seqs:
            raise Exception('Task sequence does not exist.')

        if len(seqs) > 1:
            raise Exception('More than one task sequences found.')

        return seqs[0]

    def get_sequence_attributes(self, attribute):
        return nx.get_edge_attributes(self._graph, attribute)

    def add_sequence(self, source, destination, criteria=None):
        if not self.has_task(source):
            self.add_task(source)

        if not self.has_task(destination):
            self.add_task(destination)

        seqs = self.has_sequence(source, destination, criteria)

        if len(seqs) > 1:
            raise Exception('More than one task sequences found.')

        if not seqs:
            self._graph.add_edge(source, destination, criteria=criteria)
        else:
            self.update_sequence(
                source,
                destination,
                key=seqs[0][2],
                criteria=criteria
            )

    def update_sequence(self, source, destination, key, **kwargs):
        seq = self.get_sequence(source, destination, key=key)

        for attr, value in six.iteritems(kwargs):
            self._graph[source][destination][seq[2]][attr] = value

    def get_next_sequences(self, task):
        return sorted(
            [e for e in self._graph.out_edges([task], data=True, keys=True)],
            key=lambda x: x[1]
        )

    def get_prev_sequences(self, task):
        return sorted(
            [e for e in self._graph.in_edges([task], data=True, keys=True)],
            key=lambda x: x[1]
        )

    def in_cycle(self, task):
        return [c for c in nx.simple_cycles(self._graph) if task in c]

    def is_join_task(self, task):
        return self._graph.node[task].get('join') is not None

    def is_split_task(self, task):
        return (
            len(self.get_prev_sequences(task)) > 1 and
            not self.is_join_task(task)
        )
