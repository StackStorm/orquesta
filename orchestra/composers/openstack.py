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
import six
from six.moves import queue

from orchestra.composers import base
from orchestra import composition
from orchestra.utils import expression


LOG = logging.getLogger(__name__)


class MistralWorkflowComposer(base.WorkflowComposer):

    @staticmethod
    def is_join_task(task_spec):
        return task_spec.get('join') is not None

    @staticmethod
    def get_next_tasks(task_spec, condition):
        return [
            list(task.items())[0] if isinstance(task, dict) else (task, None)
            for task in task_spec.get(condition, [])
        ]

    @staticmethod
    def compose_task_transition_criteria(task_name, task_state, expr=None):
        yaql_expr = (
            'task(%s).get(state, "unknown") = "%s"' % (task_name, task_state)
        )

        if expr:
            yaql_expr += ' and (%s)' % expression.strip_delimiter(expr)

        return {'yaql': yaql_expr}

    @classmethod
    def compose(cls, definition, entry=None):
        if not definition:
            raise ValueError('Workflow definition is empty.')

        if not entry and len(definition) == 1:
            entry = definition.keys()[0]

        task_specs = definition[entry]['tasks']
        scores = {entry: composition.WorkflowScore()}
        q = queue.Queue()

        for task_name in task_specs.keys():
            q.put((task_name, entry))

        while not q.empty():
            task_name, score = q.get()
            task_spec = task_specs[task_name]

            scores[score].add_task(task_name)

            if cls.is_join_task(task_spec):
                scores[score].update_task(task_name, join=True)

            next_tasks = cls.get_next_tasks(task_spec, 'on-success')

            for next_task_name, expr in next_tasks:
                criteria = cls.compose_task_transition_criteria(
                    task_name,
                    'succeeded',
                    expr=expr
                )

                scores[score].add_sequence(
                    task_name,
                    next_task_name,
                    criteria=criteria
                )

        return scores

    @classmethod
    def serialize(cls, scores):
        return {name: score.show() for name, score in six.iteritems(scores)}
