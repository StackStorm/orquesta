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
from six.moves import queue

from orchestra.composers import base
from orchestra import composition
from orchestra import states
from orchestra.specs import v2 as specs


LOG = logging.getLogger(__name__)


class ReverseWorkflowComposer(base.WorkflowComposer):
    wf_spec_type = specs.ReverseWorkflowSpec

    @classmethod
    def _compose_sequence_criteria(cls, task_name, *args, **kwargs):
        criteria = []

        task_state_criterion = (
            'task_state(%s) = %s' % (
                task_name,
                str(states.SUCCESS)
            )
        )

        criteria.append('<% ' + task_state_criterion + ' %>')

        return criteria

    @classmethod
    def _compose_wf_graph(cls, wf_spec):
        if not isinstance(wf_spec, cls.wf_spec_type):
            raise TypeError(
                'Workflow spec is not typeof %s.' % cls.wf_spec_type.__name__
            )

        if wf_spec.has_cycles():
            raise Exception('Cycle detected in reverse workflow.')

        q = queue.Queue()
        wf_graph = composition.WorkflowGraph()

        for task_name in wf_spec.get_start_tasks():
            q.put(task_name)

        while not q.empty():
            task_name = q.get()

            wf_graph.add_task(task_name)

            if wf_spec.is_join_task(task_name):
                wf_graph.update_task(task_name, join='all')

            next_tasks = wf_spec.get_next_tasks(task_name)

            for next_task_name, expr, condition in next_tasks:
                if not wf_graph.has_task(next_task_name):
                    q.put(next_task_name)

                criteria = cls._compose_sequence_criteria(task_name)

                seqs = wf_graph.has_sequence(
                    task_name,
                    next_task_name,
                    criteria=criteria
                )

                if not seqs:
                    wf_graph.add_sequence(
                        task_name,
                        next_task_name,
                        criteria=criteria
                    )

        return wf_graph
