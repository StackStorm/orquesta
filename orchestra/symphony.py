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
import uuid

from orchestra import composition
from orchestra.utils import expression


LOG = logging.getLogger(__name__)


class WorkflowConductor(object):

    def __init__(self, scores, entry=None, execution=None):
        if len(scores) < 1:
            raise Exception('No workflow score is provided.')

        if len(scores) > 1 and not entry:
            raise Exception('No entry point for multiple workflow scores.')

        if len(scores) == 1 and entry is not None and entry not in scores:
            raise Exception('Unable to find entry point in workflow scores.')

        self.scores = scores
        self.entry = entry if entry else list(scores.keys())[0]
        self.wf_ex = execution if execution else composition.WorkflowExecution()

    def add_task(self, score, task_name, prev_task_ex_ids=None):
        task_ex_id = uuid.uuid4().hex

        self.wf_ex.add_task(task_ex_id, name=task_name, score=score)

        if prev_task_ex_ids:
            for prev_task_ex_id in prev_task_ex_ids:
                self.wf_ex.add_sequence(prev_task_ex_id, task_ex_id)

        return {
            'id': task_ex_id,
            'name': task_name,
            'score': score
        }

    def start_workflow(self):
        return [
            self.add_task(self.entry, task_name)
            for task_name in self.scores[self.entry].get_start_tasks()
        ]

    def on_task_complete(self, task, context=None):
        self.wf_ex.update_task(task['id'], state=task['state'])

        if not context:
            context = {'__tasks': {}}

        if '__tasks' not in context:
            context['__tasks'] = {}

        context['__tasks'][task['name']] = task

        tasks = []
        score = self.scores[task['score']]
        next_seqs = [
            edge for edge in score._graph.out_edges([task['name']], data=True)
            if expression.evaluate(edge[2]['criteria']['yaql'], context)
        ]

        for next_seq in next_seqs:
            next_task = next_seq[1]

            if not dict(score._graph.nodes(data=True))[next_task].get('join'):
                tasks.append(
                    self.add_task(
                        task['score'],
                        next_task,
                        [task['id']]
                    )
                )
            else:
                prev_seqs = score._graph.in_edges([next_task], data=True)
                prev_tasks = [seq[0] for seq in prev_seqs]

                prev_task_ex_ids = [
                    n for n, d in self.wf_ex._graph.nodes_iter(data=True)
                    if (d['score'] == task['score'] and
                        d['name'] in prev_tasks and
                        d.get('state') == 'succeeded')
                ]

                if len(prev_seqs) == len(prev_task_ex_ids):
                    tasks.append(
                        self.add_task(
                            task['score'],
                            next_task,
                            prev_task_ex_ids
                        )
                    )

        return tasks
