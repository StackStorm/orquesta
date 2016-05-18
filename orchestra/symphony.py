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

from orchestra import composition
from orchestra.utils import expression


LOG = logging.getLogger(__name__)


class WorkflowConductor(object):

    def __init__(self, wf_ex_graph):
        if not wf_ex_graph:
            raise ValueError('Workflow execution graph is not provided.')

        if not isinstance(wf_ex_graph, composition.WorkflowGraph):
            raise ValueError('Invalid type for workflow execution graph.')

        self.wf_ex_graph = wf_ex_graph

    def start_workflow(self):
        return self.wf_ex_graph.get_start_tasks()

    def on_task_complete(self, task, context=None):
        self.wf_ex_graph.update_task(task['id'], state=task['state'])

        if not context:
            context = {'__tasks': {}}

        if '__tasks' not in context:
            context['__tasks'] = {}

        context['__tasks'][task['name']] = task

        tasks = []
        nodes = dict(self.wf_ex_graph._graph.nodes(data=True))
        outbound_transitions = [
            e
            for e in self.wf_ex_graph._graph.out_edges([task['id']], data=True)
            if expression.evaluate(e[2]['criteria']['yaql'], context)
        ]

        for transition in outbound_transitions:
            next_task_id, attributes = transition[1], transition[2]

            if not attributes.get('satisfied', False):
                edge = self.wf_ex_graph._graph[task['id']][next_task_id][0]
                edge['satisfied'] = True

            is_join_task = nodes[next_task_id].get('join')

            if is_join_task:
                inbound_transitions = self.wf_ex_graph._graph.in_edges(
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
                'name': nodes[next_task_id]['name']
            }

            tasks.append(next_task)

        return sorted(tasks, key=lambda x: x['name'])
