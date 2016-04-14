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
import Queue
import six

from orchestra.composers import base
from orchestra import composition


LOG = logging.getLogger(__name__)


class MistralWorkflowComposer(base.WorkflowComposer):

    @staticmethod
    def is_next_task(task_name, next_tasks):
        task_names = [
            next_task for next_task in next_tasks
            if not isinstance(next_task, dict)
        ]

        task_names += [
            next_task.keys()[0]
            for next_task in next_tasks
            if isinstance(next_task, dict)
        ]

        return task_name in task_names

    @staticmethod
    def is_join_task(task_spec):
        return task_spec.get('join') is not None

    @classmethod
    def get_prev_tasks(cls, target, workflow):
        return [
            task_name
            for task_name, task_spec in six.iteritems(workflow['tasks'])
            if (cls.is_next_task(target, task_spec.get('on-success', [])) or
                cls.is_next_task(target, task_spec.get('on-error', [])) or
                cls.is_next_task(target, task_spec.get('on-complete', [])))
        ]

    @staticmethod
    def get_next_tasks(task_spec):
        return [
            task.keys()[0] if isinstance(task, dict) else task
            for task in task_spec.get('on-success', [])
        ]

    @classmethod
    def compose(cls, definition, entry=None):
        if not definition:
            raise ValueError('Workflow definition is empty.')

        if not entry and len(definition) == 1:
            entry = definition.keys()[0]

        task_specs = definition[entry]['tasks']

        scores = {
            entry: composition.WorkflowScore()
        }

        upstream_map = {
            task_name: cls.get_prev_tasks(task_name, definition[entry])
            for task_name, task_spec in six.iteritems(task_specs)
        }

        tasks_with_multi_parents = [
            task_name
            for task_name, prev_tasks in six.iteritems(upstream_map)
            if len(prev_tasks) > 1
        ]

        tasks_with_single_parent = [
            task_name
            for task_name, prev_tasks in six.iteritems(upstream_map)
            if len(prev_tasks) == 1
        ]

        tasks_with_no_parent = [
            task_name
            for task_name, prev_tasks in six.iteritems(upstream_map)
            if len(prev_tasks) < 1
        ]

        q = Queue.Queue()

        for task_name in tasks_with_no_parent:
            q.put((task_name, entry))

        while not q.empty():
            task_name, score = q.get()

            if task_name in tasks_with_no_parent:
                scores[score].add_task(task_name)
            elif task_name in tasks_with_single_parent:
                scores[score].add_sequence(
                    upstream_map[task_name][0],
                    task_name
                )
            elif task_name in tasks_with_multi_parents:
                if cls.is_join_task(task_specs[task_name]):
                    for prev_task_name in upstream_map[task_name]:
                        scores[score].add_sequence(
                            prev_task_name,
                            task_name
                        )
                else:
                    for prev_task_name in upstream_map[task_name]:
                        fqtn = prev_task_name + '->' + task_name
                        scores[score].add_sequence(prev_task_name, fqtn)
                        subscore = score + '.' + task_name
                        scores[subscore] = composition.WorkflowScore()
                        scores[subscore].add_task(task_name)

                    score = subscore

            for next_task_name in cls.get_next_tasks(task_specs[task_name]):
                q.put((next_task_name, score))

        return scores

    @classmethod
    def serialize(cls, scores):
        return {name: score.show() for name, score in six.iteritems(scores)}
