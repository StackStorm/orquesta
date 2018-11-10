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

from orquesta import exceptions as exc
from orquesta.expressions import base as expr
from orquesta.specs.mistral.v2 import base
from orquesta.specs.mistral.v2 import tasks
from orquesta.specs.mistral.v2 import types as mistral_types
from orquesta.specs import types
from orquesta.utils import dictionary as dx


LOG = logging.getLogger(__name__)


def instantiate(definition):
    definition.pop('version', None)

    if len(definition.keys()) > 1:
        raise ValueError('Workflow definition contains more than one workflow.')

    wf_name, wf_spec = list(definition.items())[0]

    return WorkflowSpec(wf_spec, name=wf_name)


def deserialize(data):
    return WorkflowSpec.deserialize(data)


class WorkflowSpec(base.Spec):
    _schema = {
        'type': 'object',
        'properties': {
            'type': mistral_types.WORKFLOW_TYPE,
            'vars': types.NONEMPTY_DICT,
            'input': types.UNIQUE_STRING_OR_ONE_KEY_DICT_LIST,
            'output': types.NONEMPTY_DICT,
            'output-on-error': types.NONEMPTY_DICT,
            'task-defaults': tasks.TaskDefaultsSpec,
            'tasks': tasks.TaskMappingSpec
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
        'vars'
    ]

    def render_input(self, runtime_inputs, in_ctx=None):
        input_specs = getattr(self, 'input') or []
        default_inputs = dict([list(i.items())[0] for i in input_specs if isinstance(i, dict)])
        merged_inputs = dx.merge_dicts(default_inputs, runtime_inputs, True)
        rendered_inputs = {}
        errors = []

        try:
            rendered_inputs = expr.evaluate(merged_inputs, {})
        except exc.ExpressionEvaluationException as e:
            errors.append(str(e))

        return rendered_inputs, errors

    def render_vars(self, in_ctx):
        vars_specs = getattr(self, 'vars') or {}
        rendered_vars = {}
        errors = []

        try:
            rendered_vars = expr.evaluate(vars_specs, in_ctx)
        except exc.ExpressionEvaluationException as e:
            errors.append(str(e))

        return rendered_vars, errors

    def render_output(self, in_ctx):
        output_specs = getattr(self, 'output') or {}
        rendered_outputs = {}
        errors = []

        try:
            rendered_outputs = {
                var_name: expr.evaluate(var_expr, in_ctx)
                for var_name, var_expr in six.iteritems(output_specs)
            }
        except exc.ExpressionEvaluationException as e:
            errors.append(str(e))

        return rendered_outputs, errors


class WorkbookSpec(base.Spec):
    _schema = {
        'type': 'object',
        'properties': {
            'workflows': {
                'type': 'object',
                'minProperties': 1,
                'patternProperties': {
                    '^(?!version)\w+$': WorkflowSpec
                }
            }
        },
        'additionalProperties': False
    }
