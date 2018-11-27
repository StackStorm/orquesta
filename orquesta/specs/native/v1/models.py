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
import six
from six.moves import queue
import yaml

from orquesta import events
from orquesta import exceptions as exc
from orquesta.expressions import base as expr
from orquesta.specs.native.v1 import base
from orquesta.specs import types
from orquesta.utils import context as ctx
from orquesta.utils import dictionary as dx
from orquesta.utils import parameters as args_utils


LOG = logging.getLogger(__name__)

RESERVED_TASK_NAMES = list(events.ENGINE_EVENT_MAP.keys())


def instantiate(definition):
    return WorkflowSpec(definition)


def deserialize(data):
    return WorkflowSpec.deserialize(data)


class TaskTransitionSpec(base.Spec):
    _schema = {
        'type': 'object',
        'properties': {
            'when': types.NONEMPTY_STRING,
            'publish': {
                'oneOf': [
                    types.NONEMPTY_STRING,
                    types.UNIQUE_ONE_KEY_DICT_LIST
                ]
            },
            'do': {
                'oneOf': [
                    types.NONEMPTY_STRING,
                    types.UNIQUE_STRING_LIST
                ]
            }
        },
        'additionalProperties': False
    }

    _context_evaluation_sequence = [
        'when',
        'publish',
        'do'
    ]

    _context_inputs = [
        'publish'
    ]

    def __init__(self, *args, **kwargs):
        super(TaskTransitionSpec, self).__init__(*args, **kwargs)

        publish_spec = getattr(self, 'publish', None)

        if publish_spec and isinstance(publish_spec, six.string_types):
            self.publish = args_utils.parse_inline_params(publish_spec or str())

        do_spec = getattr(self, 'do', None)

        if not do_spec:
            self.do = 'noop'


class TaskTransitionSequenceSpec(base.SequenceSpec):
    _schema = {
        'type': 'array',
        'items': TaskTransitionSpec
    }


class ItemizedSpec(base.Spec):
    _items_regex = (
        # Regular expression in the form "x, y, z, ... in <expression>"
        # or "x in <expression>" with optional space(s) on both end.
        '^(\s+)?({expr})(\s+)?$|^(\s+)?((\w+,\s?|\s+)+)?(\w+)\s+in\s+({expr})(\s+)?$'.format(
            expr='|'.join(expr.get_statement_regexes().values())
        )
    )

    _schema = {
        'type': 'object',
        'properties': {
            'items': {
                'type': 'string',
                'minLength': 1,
                'pattern': _items_regex
            },
            'concurrency': types.STRING_OR_POSITIVE_INTEGER
        }
    }

    _context_evaluation_sequence = [
        'items',
        'concurrency'
    ]


class TaskSpec(base.Spec):
    _schema = {
        'type': 'object',
        'properties': {
            'delay': types.STRING_OR_POSITIVE_INTEGER,
            'join': {
                'oneOf': [
                    {'enum': ['all']},
                    types.POSITIVE_INTEGER
                ]
            },
            'with': ItemizedSpec,
            'action': types.NONEMPTY_STRING,
            'input': {
                'oneOf': [
                    types.NONEMPTY_STRING,
                    types.NONEMPTY_DICT,
                ]
            },
            'next': TaskTransitionSequenceSpec,
        },
        'additionalProperties': False
    }

    _context_evaluation_sequence = [
        'delay',
        'with',
        'action',
        'input',
        'next'
    ]

    def __init__(self, *args, **kwargs):
        super(TaskSpec, self).__init__(*args, **kwargs)

        action_spec = getattr(self, 'action', str())
        input_spec = args_utils.parse_inline_params(action_spec, preserve_order=False)

        if input_spec:
            self.action = action_spec[:action_spec.index(' ')]
            self.input = input_spec

    def has_items(self):
        return hasattr(self, 'with') and getattr(self, 'with', None) is not None

    def get_items_spec(self):
        return getattr(self, 'with', None)

    def has_join(self):
        return hasattr(self, 'join') and self.join

    def render(self, in_ctx):
        action_specs = []

        if not self.has_items():
            action_spec = {
                'action': expr.evaluate(self.action, in_ctx),
                'input': expr.evaluate(getattr(self, 'input', {}), in_ctx)
            }

            action_specs.append(action_spec)
        else:
            items_spec = self.get_items_spec()

            items_expr = (
                items_spec.items.strip() if 'in' not in items_spec.items
                else items_spec.items[items_spec.items.index('in') + 2:].strip()
            )

            items = expr.evaluate(items_expr, in_ctx)

            if not isinstance(items, list):
                raise TypeError('The value of "%s" is not type of list.' % items_expr)

            item_keys = (
                None if 'in' not in items_spec.items
                else items_spec.items[:items_spec.items.index('in')].replace(' ', '').split(',')
            )

            for idx, item in enumerate(items):
                if item_keys and (isinstance(item, tuple) or isinstance(item, list)):
                    item = dict(zip(item_keys, list(item)))
                elif item_keys and len(item_keys) == 1:
                    item = {item_keys[0]: item}

                item_ctx_value = ctx.set_current_item(in_ctx, item)

                action_spec = {
                    'action': expr.evaluate(self.action, item_ctx_value),
                    'input': expr.evaluate(getattr(self, 'input', {}), item_ctx_value),
                    'item_id': idx
                }

                action_specs.append(action_spec)

        return self, action_specs

    def finalize_context(self, next_task_name, task_transition_meta, in_ctx):
        rolling_ctx = copy.deepcopy(in_ctx)
        new_ctx = {}
        errors = []

        task_transition_specs = getattr(self, 'next') or []
        task_transition_spec = task_transition_specs[task_transition_meta[3]['ref']]
        next_task_names = getattr(task_transition_spec, 'do') or []

        if next_task_name in next_task_names:
            for task_publish_spec in (getattr(task_transition_spec, 'publish') or {}):
                var_name = list(task_publish_spec.items())[0][0]
                default_var_value = list(task_publish_spec.items())[0][1]

                try:
                    rendered_var_value = expr.evaluate(default_var_value, rolling_ctx)
                    rolling_ctx[var_name] = rendered_var_value
                    new_ctx[var_name] = rendered_var_value
                except exc.ExpressionEvaluationException as e:
                    errors.append(e)

        out_ctx = dx.merge_dicts(in_ctx, new_ctx, overwrite=True)

        for key in list(out_ctx.keys()):
            if key.startswith('__'):
                out_ctx.pop(key)

        return out_ctx, errors


class TaskMappingSpec(base.MappingSpec):
    _schema = {
        'type': 'object',
        'minProperties': 1,
        'patternProperties': {
            '^\w+$': TaskSpec
        }
    }

    def has_task(self, task_name):
        if task_name in RESERVED_TASK_NAMES:
            return True

        return task_name in self

    def get_task(self, task_name):
        if task_name in RESERVED_TASK_NAMES:
            return TaskSpec({'name': task_name})

        return self[task_name]

    def get_next_tasks(self, task_name, *args, **kwargs):
        task_spec = self.get_task(task_name)

        next_tasks = []

        task_transitions = getattr(task_spec, 'next') or []

        for task_transition_item_idx, task_transition in enumerate(task_transitions):
            condition = getattr(task_transition, 'when') or None
            next_task_names = getattr(task_transition, 'do') or []

            if isinstance(next_task_names, six.string_types):
                next_task_names = [x.strip() for x in next_task_names.split(',')]

            for next_task_name in next_task_names:
                next_tasks.append((next_task_name, condition, task_transition_item_idx))

        return sorted(next_tasks, key=lambda x: x[0])

    def get_prev_tasks(self, task_name, *args, **kwargs):
        prev_tasks = []

        for name, task_spec in six.iteritems(self):
            for next_task in self.get_next_tasks(name):
                if task_name == next_task[0]:
                    prev_tasks.append((name, next_task[1], next_task[2]))

        return sorted(prev_tasks, key=lambda x: x[0])

    def get_start_tasks(self):
        start_tasks = [
            (task_name, None, None)
            for task_name in self.keys()
            if not self.get_prev_tasks(task_name)
        ]

        return sorted(start_tasks, key=lambda x: x[0])

    def is_join_task(self, task_name):
        task_spec = self.get_task(task_name)

        return getattr(task_spec, 'join', None) is not None

    def is_split_task(self, task_name):
        return (
            not self.is_join_task(task_name) and
            len(self.get_prev_tasks(task_name)) > 1
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
                continue

            for task in self.get_next_tasks(next_task_name):
                q.put(task[0])

            traversed.append(next_task_name)

        return False

    def has_cycles(self):
        for task_name, task_spec in six.iteritems(self):
            if self.in_cycle(task_name):
                return True

        return False

    def inspect_semantics(self, parent=None):
        result = []
        traversed = []

        # Identify use of reserved words in task names.
        for task_name, task_spec in six.iteritems(self):
            if task_name in RESERVED_TASK_NAMES:
                message = 'The task name "%s" is reserved with special function.' % task_name
                spec_path = parent.get('spec_path') + '.' + task_name
                schema_path = parent.get('schema_path') + '.patternProperties.^\\w+$'
                entry = {'message': message, 'spec_path': spec_path, 'schema_path': schema_path}
                result.append(entry)

        # Identify the undefined task in task transitions.
        q = queue.Queue()

        for task in self.get_start_tasks():
            q.put(task[0])

        while not q.empty():
            task_name = q.get()
            traversed.append(task_name)

            # Identify the next set of tasks and related transition specs.
            # The get_next_tasks function is not used here because it doesn't
            # provide the specific info required to identify spec/schema paths.
            task_spec = self.get_task(task_name)
            task_transition_specs = getattr(task_spec, 'next') or []
            spec_path = parent.get('spec_path') + '.' + task_name
            schema_path = parent.get('schema_path') + '.patternProperties.^\\w+$'

            for i in range(0, len(task_transition_specs)):
                task_transition_spec = task_transition_specs[i]
                next_task_names = getattr(task_transition_spec, 'do') or []

                if isinstance(next_task_names, six.string_types):
                    next_task_names = [x.strip() for x in next_task_names.split(',')]

                for next_task_name in next_task_names:
                    # If the next task has already been traversed, then skip.
                    if next_task_name in traversed:
                        continue

                    if self.has_task(next_task_name):
                        if next_task_name not in RESERVED_TASK_NAMES + traversed:
                            q.put(next_task_name)
                    else:
                        entry = {
                            'message': 'The task "%s" is not defined.' % next_task_name,
                            'spec_path': spec_path + '.next[' + str(i) + '].do',
                            'schema_path': schema_path + '.properties.next.items.properties.do'
                        }

                        result.append(entry)

        return result

    def inspect_context(self, parent=None):
        ctxs = {}
        errors = []
        traversed = []
        parent_ctx = parent.get('ctx', []) if parent else []
        rolling_ctx = list(set(parent_ctx))
        q = queue.Queue()

        for task in self.get_start_tasks():
            q.put((task[0], copy.deepcopy(rolling_ctx)))

        while not q.empty():
            task_name, task_ctx = q.get()
            traversed.append(task_name)

            if not task_ctx:
                task_ctx = ctxs.get(task_name, [])

            task_spec = self.get_task(task_name)

            spec_path = parent.get('spec_path') + '.' + task_name
            schema_path = parent.get('schema_path') + '.patternProperties.^\\w+$'

            task_parent = {
                'ctx': task_ctx,
                'spec_path': spec_path,
                'schema_path': schema_path
            }

            result = task_spec.inspect_context(parent=task_parent)
            errors.extend(result[0])
            task_ctx = list(set(task_ctx + result[1]))
            rolling_ctx = list(set(rolling_ctx + task_ctx))

            # Identify the next set of tasks and related transition specs.
            transitions = []
            task_transition_specs = getattr(task_spec, 'next') or []

            for i in range(0, len(task_transition_specs)):
                task_transition_spec = task_transition_specs[i]
                next_task_names = getattr(task_transition_spec, 'do') or []

                if not next_task_names:
                    transitions.append((None, task_transition_spec, str(i)))
                    continue

                if isinstance(next_task_names, six.string_types):
                    next_task_names = [x.strip() for x in next_task_names.split(',')]

                for next_task_name in next_task_names:
                    entry = (next_task_name, task_transition_spec, str(i))
                    transitions.append(entry)

            for entry in transitions:
                next_task_name = entry[0]
                task_transition_spec = entry[1]
                seq_num = entry[2]

                parent_ctx = {
                    'ctx': task_ctx,
                    'spec_path': spec_path + '.next[' + seq_num + ']',
                    'schema_path': schema_path + '.properties.next.items'
                }

                result = task_transition_spec.inspect_context(parent_ctx)
                errors.extend(result[0])
                branch_ctx = list(set(task_ctx + result[1]))

                if (not next_task_name or
                        next_task_name in traversed or
                        not self.has_task(next_task_name)):
                    continue

                next_task_spec = self.get_task(next_task_name)

                if not next_task_spec.has_join():
                    q.put((next_task_name, branch_ctx))
                else:
                    next_task_ctx = ctxs.get(next_task_name, [])
                    ctxs[next_task_name] = list(set(next_task_ctx + branch_ctx))
                    q.put((next_task_name, None))

        return (errors, rolling_ctx)


class WorkflowSpec(base.Spec):
    _schema = {
        'type': 'object',
        'properties': {
            'vars': types.UNIQUE_ONE_KEY_DICT_LIST,
            'input': types.UNIQUE_STRING_OR_ONE_KEY_DICT_LIST,
            'output': types.UNIQUE_ONE_KEY_DICT_LIST,
            'tasks': TaskMappingSpec
        },
        'required': ['tasks'],
        'additionalProperties': False
    }

    _context_evaluation_sequence = [
        'input',
        'vars',
        'tasks',
        'output'
    ]

    _context_inputs = [
        'input',
        'vars',
        'output'
    ]

    def __init__(self, spec, name=None, member=False):
        if not spec:
            raise ValueError('The spec cannot be type of None.')

        spec = (
            yaml.safe_load(spec)
            if not isinstance(spec, dict) and not isinstance(spec, list)
            else spec
        )

        # Resolve shorthand inline with items.
        if 'tasks' in spec and isinstance(spec['tasks'], dict) and spec['tasks']:
            for task_name, task_spec in six.iteritems(spec['tasks']):
                if 'with' in task_spec and isinstance(task_spec['with'], six.string_types):
                    task_spec['with'] = {'items': task_spec['with']}

        super(WorkflowSpec, self).__init__(spec, name=name, member=member)

    def render_input(self, runtime_inputs, in_ctx=None):
        rolling_ctx = copy.deepcopy(in_ctx) if in_ctx else {}
        errors = []

        for input_spec in (getattr(self, 'input') or []):
            if isinstance(input_spec, dict):
                input_name = list(input_spec.items())[0][0]
                default_input_value = list(input_spec.items())[0][1]
            else:
                input_name = input_spec
                default_input_value = None

            runtime_input_value = runtime_inputs.get(input_name, default_input_value)

            try:
                rendered_input_value = expr.evaluate(runtime_input_value, rolling_ctx)
                rolling_ctx[input_name] = rendered_input_value
            except exc.ExpressionEvaluationException as e:
                errors.append(e)

        return rolling_ctx, errors

    def render_vars(self, in_ctx):
        rolling_ctx = copy.deepcopy(in_ctx)
        rendered_vars = {}
        errors = []

        for var_spec in (getattr(self, 'vars') or []):
            var_name = list(var_spec.items())[0][0]
            default_var_value = list(var_spec.items())[0][1]

            try:
                rendered_var_value = expr.evaluate(default_var_value, rolling_ctx)
                rolling_ctx[var_name] = rendered_var_value
                rendered_vars[var_name] = rendered_var_value
            except exc.ExpressionEvaluationException as e:
                errors.append(e)

        return rendered_vars, errors

    def render_output(self, in_ctx):
        output_specs = getattr(self, 'output') or []
        rolling_ctx = copy.deepcopy(in_ctx)
        rendered_outputs = {}
        errors = []

        for output_spec in output_specs:
            output_name = list(output_spec.items())[0][0]
            default_output_value = list(output_spec.items())[0][1]

            try:
                rendered_output_value = expr.evaluate(default_output_value, rolling_ctx)
                rolling_ctx[output_name] = rendered_output_value
                rendered_outputs[output_name] = rendered_output_value
            except exc.ExpressionEvaluationException as e:
                errors.append(e)

        return rendered_outputs, errors
