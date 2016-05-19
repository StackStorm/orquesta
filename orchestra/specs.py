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
import yaml


LOG = logging.getLogger(__name__)


class WorkflowSpec(object):

    def __init__(self, definition):
        self.definition = (
            definition
            if isinstance(definition, dict)
            else yaml.safe_load(definition)
        )

        for wf_name, wf_spec in six.iteritems(self.definition):
            if wf_name == 'version':
                continue

            self.entry = wf_name
            self.wf_spec = wf_spec
            self.task_specs = self.wf_spec.get('tasks', dict())
            break

    def get_next_tasks(self, task_name, conditions=None):
        if task_name not in self.task_specs:
            raise Exception('Task "%s" is not in the spec.' % task_name)

        task_spec = self.task_specs[task_name]

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

    def get_prev_tasks(self, task_name, conditions=None):
        prev_tasks = []

        for referrer, task_spec in six.iteritems(self.task_specs):
            for next_task in self.get_next_tasks(referrer, conditions):
                if task_name == next_task[0]:
                    prev_tasks.append(
                        (referrer, next_task[1], next_task[2])
                    )

        return sorted(prev_tasks, key=lambda x: x[0])

    def get_start_tasks(self):
        return sorted(
            [
                task_name
                for task_name in self.task_specs.keys()
                if not self.get_prev_tasks(task_name)
            ]
        )

    def is_join_task(self, task_name):
        if task_name not in self.task_specs:
            raise Exception('Task "%s" is not in the spec.' % task_name)

        return self.task_specs[task_name].get('join') is not None
