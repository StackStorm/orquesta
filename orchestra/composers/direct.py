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
from orchestra.expressions import base as expressions


LOG = logging.getLogger(__name__)


class DirectWorkflowComposer(base.WorkflowComposer):
    wf_spec_cls = specs.DirectWorkflowSpec
    expr_evaluator = expressions.get_evaluator('yaql')

    @classmethod
    def _compose_sequence_criteria(cls, task_name, *args, **kwargs):
        condition = kwargs.get('condition')
        expr = kwargs.get('expr')

        yaql_expr = (
            'task_state(%s) in %s' % (
                task_name,
                str(states.TASK_TRANSITION_MAP[condition])
            )
        )

        if expr:
            stripped_expr = cls.expr_evaluator.strip_delimiter(expr)
            yaql_expr += ' and (%s)' % stripped_expr

        return '<% ' + yaql_expr + ' %>'

    @classmethod
    def _compose_wf_graph(cls, wf_spec):
        if not isinstance(wf_spec, cls.wf_spec_cls):
            raise TypeError(
                'Workflow spec is not typeof %s.' % cls.wf_spec_cls.__name__
            )

        q = queue.Queue()
        wf_graph = composition.WorkflowGraph()

        for task_name in wf_spec.get_start_tasks():
            q.put((task_name, []))

        while not q.empty():
            task_name, splits = q.get()

            wf_graph.add_task(task_name)

            if wf_spec.is_join_task(task_name):
                task_spec = wf_spec.get_task(task_name)
                wf_graph.update_task(task_name, join=task_spec['join'])

            # Determine if the task is a split task and if it is in a cycle.
            # If the task is a split task, keep track of where the split(s)
            # occurs.
            if (wf_spec.is_split_task(task_name) and
                    not wf_spec.in_cycle(task_name)):
                splits.append(task_name)

            if splits:
                wf_graph.update_task(task_name, splits=splits)

            next_tasks = wf_spec.get_next_tasks(task_name)

            for next_task_name, expr, condition in next_tasks:
                if (not wf_graph.has_task(next_task_name) or
                        not wf_spec.in_cycle(next_task_name)):
                    q.put((next_task_name, list(splits)))

                criteria = cls._compose_sequence_criteria(
                    task_name,
                    condition=condition,
                    expr=expr
                )

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
