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

import copy
import logging
import six
from six.moves import queue
import uuid

from orchestra.composers import base
from orchestra import composition
from orchestra import states
from orchestra.utils import expression


LOG = logging.getLogger(__name__)


class MistralWorkflowComposer(base.WorkflowComposer):

    @staticmethod
    def _is_join_task(task_spec):
        return task_spec.get('join') is not None

    @staticmethod
    def _get_next_tasks(task_spec, conditions=None):
        if not conditions:
            conditions = [
                'on-success',
                'on-error',
                'on-complete'
            ]

        next_tasks = []

        for condition in conditions:
            for task in task_spec.get(condition, []):
                next_tasks.append(
                    list(task.items())[0] + (condition,)
                    if isinstance(task, dict)
                    else (task, None, condition)
                )

        return sorted(next_tasks, key=lambda x: x[0])

    @classmethod
    def _get_prev_tasks(cls, task_specs, task_name, conditions=None):
        prev_tasks = []

        for referrer, task_spec in six.iteritems(task_specs):
            for next_task in cls._get_next_tasks(task_spec, conditions):
                if task_name == next_task[0]:
                    prev_tasks.append(
                        (referrer, next_task[1], next_task[2])
                    )

        return sorted(prev_tasks, key=lambda x: x[0])

    @classmethod
    def _get_start_tasks(cls, task_specs):
        return [
            task_name
            for task_name, task_spec in six.iteritems(task_specs)
            if not cls._get_prev_tasks(task_specs, task_name)
        ]

    @staticmethod
    def _compose_task_transition_criteria(task_name, task_state, expr=None):
        yaql_expr = (
            'task(%s).get(state, "unknown") = "%s"' % (task_name, task_state)
        )

        if expr:
            yaql_expr += ' and (%s)' % expression.strip_delimiter(expr)

        return {'yaql': yaql_expr}

    @classmethod
    def _compose_wf_graphs(cls, definition, entry):
        task_specs = definition[entry]['tasks']
        wf_graphs = {entry: composition.WorkflowGraph()}
        q = queue.Queue()

        for task_name in cls._get_start_tasks(task_specs):
            q.put((task_name, entry))

        while not q.empty():
            task_name, wf_graph = q.get()
            task_spec = task_specs[task_name]
            prev_tasks = cls._get_prev_tasks(task_specs, task_name)
            is_join_task = cls._is_join_task(task_spec)
            is_split_task = not is_join_task and len(prev_tasks) > 1

            wf_graphs[wf_graph].add_task(task_name)

            if is_join_task:
                wf_graphs[wf_graph].update_task(task_name, join=is_join_task)

            if is_split_task:
                sub_wf_graph = entry + '.' + task_name

                wf_graphs[wf_graph].update_task(
                    task_name,
                    subgraph=sub_wf_graph
                )

                wf_graph = sub_wf_graph

                if wf_graph not in wf_graphs:
                    wf_graphs[wf_graph] = composition.WorkflowGraph()
                    wf_graphs[wf_graph].add_task(task_name)

            next_tasks = cls._get_next_tasks(task_spec)

            for next_task_name, expr, condition in next_tasks:
                if not wf_graphs[wf_graph].has_task(next_task_name):
                    q.put((next_task_name, wf_graph))

                criteria = cls._compose_task_transition_criteria(
                    task_name,
                    states.SUCCESS,
                    expr=expr
                )

                wf_graphs[wf_graph].add_sequence(
                    task_name,
                    next_task_name,
                    criteria=criteria
                )

        return wf_graphs

    @classmethod
    def _get_join_task(cls, wf_ex_graph, wf_graphs, subgraph, wf_name,
                       join_task_name, task_id):
        req = len(wf_graphs[subgraph]._graph.in_edges([join_task_name]))

        join_tasks = [
            {'id': n, 'name': d['name']}
            for n, d in wf_ex_graph._graph.nodes_iter(data=True)
            if d['name'] == join_task_name
        ]

        for join_task in join_tasks:
            in_edges = wf_ex_graph._graph.in_edges([join_task['id']])
            in_tasks = [in_edge[0] for in_edge in in_edges]

            if task_id not in in_tasks and len(in_tasks) != req:
                return join_task

        return None

    @classmethod
    def _traverse_task(cls, wf_ex_graph, wf_graphs, subgraph, wf_name,
                       task_name, prev_task_id=None):
        wf_graph = wf_graphs[subgraph]
        nodes = dict(wf_graph._graph.nodes(data=True))
        task_id = str(uuid.uuid4())

        attributes = copy.deepcopy(nodes[task_name])
        attributes['name'] = task_name
        next_subgraph = attributes.pop('subgraph', subgraph)

        wf_ex_graph.add_task(task_id, **attributes)

        if prev_task_id:
            prev_task_name = wf_ex_graph._graph.node[prev_task_id]['name']
            edges = wf_graph._graph.edge[prev_task_name][task_name]

            for edge_idx, edge_attr in six.iteritems(edges):
                attributes = copy.deepcopy(edge_attr)
                wf_ex_graph.add_sequence(prev_task_id, task_id, **attributes)

        if next_subgraph != subgraph:
            subgraph = next_subgraph
            wf_graph = wf_graphs[subgraph]
            nodes = dict(wf_graph._graph.nodes(data=True))

        for transition in wf_graph.get_next_sequences(task_name):
            next_task_name, attributes = transition[1], transition[2]
            joining = nodes[next_task_name].get('join')

            if joining:
                join_task = cls._get_join_task(
                    wf_ex_graph,
                    wf_graphs,
                    subgraph,
                    wf_name,
                    next_task_name,
                    task_id
                )

                if join_task:
                    wf_ex_graph.add_sequence(
                        task_id,
                        join_task['id'],
                        **attributes
                    )

                    continue

            cls._traverse_task(
                wf_ex_graph,
                wf_graphs,
                subgraph,
                wf_name,
                next_task_name,
                task_id
            )

    @classmethod
    def _compose_wf_ex_graph(cls, wf_graphs, entry):
        if len(wf_graphs) < 1:
            raise Exception('No workflow definition is provided.')

        if len(wf_graphs) > 1 and not entry:
            raise Exception('No entry point provided for '
                            'multiple workflow definitions.')

        if (len(wf_graphs) == 1 and
                entry is not None and entry not in wf_graphs):
            raise Exception('Unable to find entry point in '
                            'the workflow definitions.')

        wf_ex_graph = composition.WorkflowGraph()
        tasks = wf_graphs[entry].get_start_tasks()

        for task_name, attributes in sorted(six.iteritems(tasks)):
            cls._traverse_task(
                wf_ex_graph,
                wf_graphs,
                entry,
                entry,
                task_name
            )

        return wf_ex_graph

    @classmethod
    def compose(cls, definition, entry=None):
        if not definition:
            raise ValueError('Workflow definition is empty.')

        if not entry and len(definition) == 1:
            entry = definition.keys()[0]

        wf_graphs = cls._compose_wf_graphs(definition, entry=entry)

        return cls._compose_wf_ex_graph(wf_graphs, entry)
