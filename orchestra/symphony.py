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
import uuid

import six

from orchestra import composition
from orchestra.utils import expression


LOG = logging.getLogger(__name__)


class WorkflowConductor(object):

    def __init__(self, scores, entry=None, plot=None):
        if len(scores) < 1:
            raise Exception('No workflow score is provided.')

        if len(scores) > 1 and not entry:
            raise Exception('No entry point for multiple workflow scores.')

        if len(scores) == 1 and entry is not None and entry not in scores:
            raise Exception('Unable to find entry point in workflow scores.')

        self.scores = scores
        self.entry = entry if entry else list(scores.keys())[0]
        self.plot = plot if plot else self._plot_scores()

    def _get_join_task(self, plot, score_name, join_task_name, task_id):
        req = len(self.scores[score_name]._graph.in_edges([join_task_name]))

        join_tasks = [
            {'id': n, 'name': d['name'], 'score': d['score']}
            for n, d in plot._graph.nodes_iter(data=True)
            if d['name'] == join_task_name and d['score'] == score_name
        ]

        for join_task in join_tasks:
            in_edges = plot._graph.in_edges([join_task['id']])
            in_tasks = [in_edge[0] for in_edge in in_edges]

            if task_id not in in_tasks and len(in_tasks) != req:
                return join_task

        return None

    def _traverse(self, plot, score_name, task_name, prev_task_id=None):
        score = self.scores[score_name]
        nodes = dict(score._graph.nodes(data=True))
        task_id = uuid.uuid4().hex

        attributes = copy.deepcopy(nodes[task_name])
        attributes['score'] = score_name
        attributes['name'] = task_name

        plot.add_task(task_id, **attributes)

        if prev_task_id:
            prev_task_name = plot._graph.node[prev_task_id]['name']
            edges = score._graph.edge[prev_task_name][task_name]

            for edge_idx, edge_attr in six.iteritems(edges):
                attributes = copy.deepcopy(edge_attr)
                plot.add_sequence(prev_task_id, task_id, **attributes)

        transitions = [
            e for e in score._graph.out_edges([task_name], data=True)
        ]

        for transition in transitions:
            next_task_name, attributes = transition[1], transition[2]
            joining = nodes[next_task_name].get('join')

            if joining:
                join_task = self._get_join_task(
                    plot,
                    score_name,
                    next_task_name,
                    task_id
                )

                if join_task:
                    plot.add_sequence(task_id, join_task['id'], **attributes)
                    continue

            self._traverse(plot, score_name, next_task_name, task_id)

    def _plot_scores(self):
        plot = composition.WorkflowExecution()
        tasks = self.scores[self.entry].get_start_tasks()

        for task_name, attributes in six.iteritems(tasks):
            self._traverse(plot, self.entry, task_name)

        return plot

    def start_workflow(self):
        return self.plot.get_start_tasks()

    def on_task_complete(self, task, context=None):
        self.plot.update_task(task['id'], state=task['state'])

        if not context:
            context = {'__tasks': {}}

        if '__tasks' not in context:
            context['__tasks'] = {}

        context['__tasks'][task['name']] = task

        tasks = []
        nodes = dict(self.plot._graph.nodes(data=True))
        outbound_transitions = [
            e for e in self.plot._graph.out_edges([task['id']], data=True)
            if expression.evaluate(e[2]['criteria']['yaql'], context)
        ]

        for transition in outbound_transitions:
            next_task_id, attributes = transition[1], transition[2]

            if not attributes.get('satisfied', False):
                edge = self.plot._graph[task['id']][next_task_id][0]
                edge['satisfied'] = True

            is_join_task = nodes[next_task_id].get('join')

            if is_join_task:
                inbound_transitions = self.plot._graph.in_edges(
                    [next_task_id],
                    data=True
                )

                unsatisfied = [
                    t for t in inbound_transitions
                    if not t[2].get('satisfied')
                ]

                if unsatisfied:
                    continue

            next_task = {
                'id': next_task_id,
                'name': nodes[next_task_id]['name'],
                'score': nodes[next_task_id]['score']
            }

            tasks.append(next_task)

        return tasks
