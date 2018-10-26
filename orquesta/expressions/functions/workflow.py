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

from orquesta import exceptions as exc
from orquesta import states


def _get_current_task(context):
    if not context:
        raise exc.ExpressionEvaluationException('The context is not set.')

    current_task = context['__current_task'] or {}

    if not current_task:
        raise exc.ExpressionEvaluationException('The current task is not set in the context.')

    return current_task


def task_state_(context, task_id):
    if not context:
        return states.UNSET

    task_flow = context['__flow'] or {}
    task_flow_pointers = task_flow.get('tasks') or {}
    task_flow_item_idx = task_flow_pointers.get(task_id)

    if task_flow_item_idx is None:
        return states.UNSET

    task_flow_seqs = task_flow.get('sequence') or []
    task_flow_item = task_flow_seqs[task_flow_item_idx]

    if task_flow_item is None:
        return states.UNSET

    return task_flow_item.get('state', states.UNSET)


def succeeded_(context):
    current_task = _get_current_task(context)

    return (task_state_(context, current_task.get('id')) == states.SUCCEEDED)


def failed_(context):
    current_task = _get_current_task(context)

    return (task_state_(context, current_task.get('id')) == states.FAILED)


def completed_(context):
    current_task = _get_current_task(context)

    return (task_state_(context, current_task.get('id')) in states.COMPLETED_STATES)


def result_(context):
    current_task = _get_current_task(context)

    return current_task.get('result')


def item_(context, key=None):
    if not context:
        raise exc.ExpressionEvaluationException('The context is not set.')

    current_item = context['__current_item']

    if not current_item:
        return None

    if not key:
        return current_item

    if not isinstance(current_item, dict):
        raise exc.ExpressionEvaluationException('Item is not type of dict.')

    if key not in current_item:
        raise exc.ExpressionEvaluationException('Item does not have key "%s".' % key)

    return current_item[key]
