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

    def add_task(self, task, **kwargs):
        if not self.has_task(task):
            self._graph.add_node(task, **kwargs)

    def update_task(self, task, **kwargs):
        if not self.has_task(task):
            raise Exception('Task does not exist.')

        for key, value in six.iteritems(kwargs):
            self._graph.node[task][key] = value

    def has_sequence(self, source, destination):
        return self._graph.has_edge(source, destination)

    def add_sequence(self, source, destination, **kwargs):
        if not self.has_task(source):
            self.add_task(source)

        if not self.has_task(destination):
            self.add_task(destination)

        if not self.has_sequence(source, destination):
            self._graph.add_edge(source, destination, **kwargs)

    def get_start_tasks(self):
        return {
            n: copy.deepcopy(self._graph.node[n])
            for n, d in self._graph.in_degree().items() if d == 0
        }
