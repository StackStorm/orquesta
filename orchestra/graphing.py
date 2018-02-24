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

from orchestra import exceptions as exc
from orchestra.expressions import base as expressions
from orchestra.utils import dictionary as dict_utils
from orchestra import states


LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class WorkflowGraph(object):

    def __init__(self, graph=None, flow=None):
        # self._graph is the graph model for the workflow. The tracking of workflow and task
        # progress and state is separate from the graph model. There are use cases where tasks
        # may be cycled and states overwritten. Therefore, use the var _flow to track progress
        # and states.
        self._graph = graph if graph else nx.MultiDiGraph()

        # self._flow tracks the workflow and task progress, states, and flow.
        self._flow = flow if flow else dict()

        # self.flow['state'] tracks the state of the workflow execution.
        if not self._flow.get('state'):
            self._flow['state'] = None

        # self._flow['sequence'] tracks the progress and state of task executions.
        if not self._flow.get('sequence'):
            self._flow['sequence'] = list()

        # self._flow['tasks'] references current instance of tasks.
        if not self._flow.get('tasks'):
            self._flow['tasks'] = dict()

    def serialize(self):
        return {
            'graph': json_graph.adjacency_data(self._graph),
            'flow': copy.deepcopy(self._flow)
        }

    @classmethod
    def deserialize(cls, data):
        g = json_graph.adjacency_graph(copy.deepcopy(data['graph']), directed=True, multigraph=True)
        f = copy.deepcopy(data['flow'])

        return cls(graph=g, flow=f)

    def has_task(self, task_id):
        return self._graph.has_node(task_id)

    def get_task(self, task_id):
        if not self.has_task(task_id):
            raise exc.InvalidTask(task_id)

        task = {'id': task_id}
        task.update(copy.deepcopy(self._graph.node[task_id]))

        return task

    def get_task_attributes(self, attribute):
        return dict_utils.merge_dicts(
            {n: None for n in self._graph.nodes()},
            nx.get_node_attributes(self._graph, attribute),
            overwrite=True
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

    def get_start_tasks(self):
        tasks = [
            {'id': n, 'name': self._graph.node[n].get('name', n)}
            for n, d in self._graph.in_degree().items() if d == 0
        ]

        return sorted(tasks, key=lambda x: x['name'])

    def get_next_tasks(self, task_id):
        task_flow_item = self.get_task_flow_item(task_id)

        if not task_flow_item or task_flow_item.get('state') not in states.COMPLETED_STATES:
            return []

        next_tasks = []
        outbounds = self.get_next_transitions(task_id)

        for next_seq in outbounds:
            next_task_id, seq_key = next_seq[1], next_seq[2]
            task_transition_id = next_task_id + '__' + str(seq_key)

            # Evaluate if outbound criteria is satisfied.
            if not task_flow_item.get(task_transition_id):
                continue

            # Evaluate if the next task has a barrier waiting for other tasks to complete.
            if self.has_barrier(next_task_id):
                barrier = self.get_barrier(next_task_id)
                inbounds = self.get_prev_transitions(next_task_id)

                barrier = len(inbounds) if barrier == '*' else barrier
                satisfied = []

                for prev_seq in inbounds:
                    prev_task_flow_item = self.get_task_flow_item(prev_seq[0])

                    if prev_task_flow_item:
                        prev_task_transition_id = prev_seq[1] + '__' + str(prev_seq[2])
                        satisfied.append(prev_task_flow_item.get(prev_task_transition_id))

                if len(satisfied) < barrier:
                    continue

            next_task = self.get_task(next_task_id)
            next_tasks.append({'id': next_task_id, 'name': next_task['name']})

        return sorted(next_tasks, key=lambda x: x['name'])

    def has_transition(self, source, destination, criteria=None):
        return [
            edge for edge in self._graph.edges(data=True, keys=True)
            if (edge[0] == source and edge[1] == destination and
                edge[3].get('criteria', None) == criteria)
        ]

    def get_transition(self, source, destination, key=None, criteria=None):
        seqs = [
            edge for edge in self._graph.edges(data=True, keys=True)
            if (edge[0] == source and edge[1] == destination and (
                edge[3].get('criteria', None) == criteria or
                edge[2] == key))
        ]

        if not seqs:
            raise exc.InvalidTaskTransition(source, destination)

        if len(seqs) > 1:
            raise exc.AmbiguousTaskTransition(source, destination)

        return seqs[0]

    def get_transition_attributes(self, attribute):
        return nx.get_edge_attributes(self._graph, attribute)

    def add_transition(self, source, destination, criteria=None):
        if not self.has_task(source):
            self.add_task(source)

        if not self.has_task(destination):
            self.add_task(destination)

        seqs = self.has_transition(source, destination, criteria)

        if len(seqs) > 1:
            raise exc.AmbiguousTaskTransition(source, destination)

        if not seqs:
            self._graph.add_edge(source, destination, criteria=criteria)
        else:
            self.update_transition(source, destination, key=seqs[0][2], criteria=criteria)

    def update_transition(self, source, destination, key, **kwargs):
        seq = self.get_transition(source, destination, key=key)

        for attr, value in six.iteritems(kwargs):
            self._graph[source][destination][seq[2]][attr] = value

    def get_next_transitions(self, task_id):
        return sorted(
            [e for e in self._graph.out_edges([task_id], data=True, keys=True)],
            key=lambda x: x[1]
        )

    def get_prev_transitions(self, task_id):
        return sorted(
            [e for e in self._graph.in_edges([task_id], data=True, keys=True)],
            key=lambda x: x[1]
        )

    def set_barrier(self, task_id, value='*'):
        self.update_task(task_id, barrier=value)

    def get_barrier(self, task_id):
        return self.get_task(task_id).get('barrier')

    def has_barrier(self, task_id):
        b = self.get_barrier(task_id)

        return (b is not None and b != '')

    def in_cycle(self, task_id):
        return [c for c in nx.simple_cycles(self._graph) if task_id in c]

    @property
    def state(self):
        return self._flow.get('state', None)

    @state.setter
    def state(self, value):
        if not states.is_valid(value):
            raise exc.InvalidState(value)

        if not states.is_transition_valid(self.state, value):
            raise exc.InvalidStateTransition(self.state, value)

        self._flow['state'] = value

    @property
    def sequence(self):
        return self._flow['sequence']

    def get_task_flow_idx(self, task_id):
        return self._flow['tasks'].get(task_id)

    def get_task_flow_item(self, task_id):
        flow_idx = self.get_task_flow_idx(task_id)

        return self.sequence[flow_idx] if flow_idx is not None else None

    def add_task_flow_item(self, task_id):
        if not self.has_task(task_id):
            raise exc.InvalidTask(task_id)

        task_flow_item = {'id': task_id}
        self.sequence.append(task_flow_item)
        self._flow['tasks'][task_id] = len(self.sequence) - 1

        return task_flow_item

    def update_task_flow_item(self, task_id, state, context=None):
        if not states.is_valid(state):
            raise exc.InvalidState(state)

        task_flow_item = self.get_task_flow_item(task_id)

        # Create new task flow entry if it does not exist.
        if not task_flow_item:
            task_flow_item = self.add_task_flow_item(task_id)

        # If task is already completed and in cycle, then create new task flow entry.
        if self.in_cycle(task_id) and task_flow_item.get('state') in states.COMPLETED_STATES:
            task_flow_item = self.add_task_flow_item(task_id)

        # Check if the task state change is valid.
        if not states.is_transition_valid(task_flow_item.get('state'), state):
            raise exc.InvalidStateTransition(task_flow_item.get('state'), state)

        task_flow_item['state'] = state

        # Evaluate task transitions if task is in completed state.
        if state in states.COMPLETED_STATES:
            # Setup context for evaluating expressions in task transition criteria.
            ctx = dict_utils.merge_dicts(context or {}, {'__flow': self._flow}, overwrite=True)

            # Iterate thru each outbound task transitions.
            for t in self.get_next_transitions(task_id):
                criteria = t[3].get('criteria') or []
                evaluated_criteria = [expressions.evaluate(c, ctx) for c in criteria]
                task_transition_id = t[1] + '__' + str(t[2])
                task_flow_item[task_transition_id] = all(evaluated_criteria)

        return task_flow_item
