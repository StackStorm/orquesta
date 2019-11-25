# Copyright 2019 Extreme Networks, Inc.
#
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

from orquesta.expressions import base as expr_base
from orquesta import statuses
from orquesta.utils import context as ctx_util
from orquesta.utils import dictionary as dict_util


def will_retry(task_state, workflow_state, task_result=[]):
    if not isinstance(task_state, dict):
        raise TypeError('The task_state parameter must be dict type')

    if not workflow_state:
        return False

    # add context of state information and task result
    context = ctx_util.set_current_task({'__state': workflow_state.serialize()}, {
        'id': task_state['id'],
        'route': task_state['route'],
        'result': task_result,
    })

    # add context of variables which are referred by ctx() expression
    for ctx_idx in task_state['ctxs']['in']:
        context = dict_util.merge_dicts(context, workflow_state.contexts[ctx_idx], overwrite=True)

    # This returns true when specified task is eligible to be tried. This is equivalent with
    # the case that following conditions are satisfied simultaneously.
    # - The status of task is abend or succeeded.
    # - There is still retry_count left in the task_state object.
    # - The status of task and result are matched with the condition that user specified
    #   in 'when' parameter in workflow metadata.
    return ('retry_condition' in task_state and
            task_state.get('status') in statuses.COMPLETED_STATUSES and
            task_state.get('status') != statuses.CANCELED and
            task_state.get('retry_count', 0) > 0 and
            expr_base.evaluate(task_state['retry_condition'], context))
