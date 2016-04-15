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

import logging

import networkx as nx


LOG = logging.getLogger(__name__)


class WorkflowScore(object):

    def __init__(self):
        self._graph = nx.DiGraph()

    def show(self):
        return nx.to_dict_of_dicts(self._graph)

    def has_task(self, task):
        return self._graph.has_node(task)

    def add_task(self, task):
        if not self.has_task(task):
            self._graph.add_node(task)

    def has_sequence(self, source, destination):
        return self._graph.has_edge(source, destination)

    def add_sequence(self, source, destination, criteria=None):
        if not self.has_task(source):
            self.add_task(source)

        if not self.has_task(destination):
            self.add_task(destination)

        if not self.has_sequence(source, destination):
            self._graph.add_edge(
                source,
                destination,
                attr_dict=(criteria or {})
            )

    def get_start_tasks(self):
        return [n for n, d in self._graph.in_degree().items() if d == 0]
