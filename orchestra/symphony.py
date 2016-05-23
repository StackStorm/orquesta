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

        outbounds = [
            seq for seq in self.wf_ex_graph.get_next_sequences(task['id'])
            if expression.evaluate(seq[2]['criteria']['yaql'], context)
        ]

        for sequence in outbounds:
            next_task_id, attributes = sequence[1], sequence[2]
            next_task = self.wf_ex_graph.get_task(next_task_id)

            if not attributes.get('satisfied', False):
                self.wf_ex_graph.update_sequence(
                    task['id'],
                    next_task_id,
                    satisfied=True
                )

            join_spec = next_task.get('join')

            if join_spec:
                inbounds = self.wf_ex_graph.get_prev_sequences(next_task_id)
                satisfied = [t for t in inbounds if t[2].get('satisfied')]
                join_spec = len(inbounds) if join_spec == 'all' else join_spec

                if len(satisfied) < join_spec:
                    continue

            tasks.append({'id': next_task_id, 'name': next_task['name']})

        return sorted(tasks, key=lambda x: x['name'])
