# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import six
from six.moves import queue
import yaml

from orchestra.specs import types
from orchestra.specs.v2 import base
from orchestra.specs.v2 import tasks


LOG = logging.getLogger(__name__)


class WorkflowSpec(base.BaseSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'type': types.WORKFLOW_TYPE,
            'vars': types.NONEMPTY_DICT,
            'input': types.UNIQUE_STRING_OR_ONE_KEY_DICT_LIST,
            'output': types.NONEMPTY_DICT,
            'task-defaults': tasks.TaskDefaultsSpec.get_schema(includes=None),
            'tasks': tasks.TaskSpec.get_schema(includes=None)
        },
        'required': ['tasks'],
        'additionalProperties': False
    }

    def __init__(self, definition):
        if not definition:
            raise ValueError('Workflow definition is empty.')

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

    def get_task(self, task_name):
        if task_name not in self.task_specs:
            raise Exception('Task "%s" is not in the spec.' % task_name)

        return self.task_specs[task_name]

    def get_next_tasks(self, task_name, *args, **kwargs):
        raise NotImplementedError()

    def get_prev_tasks(self, task_name, *args, **kwargs):
        raise NotImplementedError()

    def get_start_tasks(self):
        return sorted(
            [
                task_name
                for task_name in self.task_specs.keys()
                if not self.get_prev_tasks(task_name)
            ]
        )

    def in_cycle(self, task_name):
        traversed = []
        q = queue.Queue()

        for task in self.get_next_tasks(task_name):
            q.put(task[0])

        while not q.empty():
            next_task_name = q.get()

            # If the next task matches the original task, then it's in a loop.
            if next_task_name == task_name:
                return True

            # If the next task has already been traversed but didn't match the
            # original task, then there's a loop but the original task is not
            # in the loop.
            if next_task_name in traversed:
                return False

            for task in self.get_next_tasks(next_task_name):
                q.put(task[0])

            traversed.append(next_task_name)

        return False

    def has_cycles(self):
        for task_name, task_spec in six.iteritems(self.task_specs):
            if self.in_cycle(task_name):
                return True

        return False


class DirectWorkflowSpec(WorkflowSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'type': {
                'enum': ['direct']
            },
            'task-defaults': tasks.DirectTaskDefaultsSpec.get_schema(None),
            'tasks': {
                'type': 'object',
                'minProperties': 1,
                'patternProperties': {
                    '^\w+$': tasks.DirectTaskSpec.get_schema(None)
                }
            }
        },
        'required': ['tasks'],
        'additionalProperties': False
    }

    def get_next_tasks(self, task_name, *args, **kwargs):
        task_spec = self.get_task(task_name)
        conditions = kwargs.get('conditions')

        if not conditions:
            conditions = [
                'on-complete',
                'on-error',
                'on-success'
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

    def get_prev_tasks(self, task_name, *args, **kwargs):
        prev_tasks = []
        conditions = kwargs.get('conditions')

        for name, task_spec in six.iteritems(self.task_specs):
            for next_task in self.get_next_tasks(name, conditions=conditions):
                if task_name == next_task[0]:
                    prev_tasks.append(
                        (name, next_task[1], next_task[2])
                    )

        return sorted(prev_tasks, key=lambda x: x[0])

    def is_join_task(self, task_name):
        task_spec = self.get_task(task_name)

        return task_spec.get('join') is not None

    def is_split_task(self, task_name):
        return (
            not self.is_join_task(task_name) and
            len(self.get_prev_tasks(task_name)) > 1
        )


class ReverseWorkflowSpec(WorkflowSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'type': {
                'enum': ['reverse']
            },
            'task-defaults': tasks.ReverseTaskDefaultsSpec.get_schema(None),
            'tasks': {
                'type': 'object',
                'minProperties': 1,
                'patternProperties': {
                    '^\w+$': tasks.ReverseTaskSpec.get_schema(None)
                }
            }
        },
        'required': ['tasks'],
        'additionalProperties': False
    }

    def get_next_tasks(self, task_name, *args, **kwargs):
        next_tasks = []

        for name, task_spec in six.iteritems(self.task_specs):
            requires = task_spec.get('requires', [])

            if isinstance(requires, six.string_types):
                requires = [requires]

            if task_name in requires:
                next_tasks.append((name, None, None))

        return sorted(next_tasks, key=lambda x: x[0])

    def get_prev_tasks(self, task_name, *args, **kwargs):
        prev_tasks = []
        task_spec = self.get_task(task_name)
        requires = task_spec.get('requires', [])

        if isinstance(requires, six.string_types):
            requires = [requires]

        for name in requires:
            prev_tasks.append((name, None, None))

        return sorted(prev_tasks, key=lambda x: x[0])

    def is_join_task(self, task_name):
        task_spec = self.get_task(task_name)
        requires = task_spec.get('requires', [])

        return len(requires) > 1


class WorkbookSpec(base.BaseSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'workflows': {
                'type': 'object',
                'minProperties': 1,
                'patternProperties': {
                    '^(?!version)\w+$': {
                        'oneOf': [
                            DirectWorkflowSpec.get_schema(includes=None),
                            ReverseWorkflowSpec.get_schema(includes=None)
                        ]
                    }
                }
            }
        },
        'additionalProperties': False
    }
