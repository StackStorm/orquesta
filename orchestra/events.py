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

from orchestra import exceptions as exc
from orchestra import states


LOG = logging.getLogger(__name__)


# Events for workflow execution.
WORKFLOW_REQUESTED = 'workflow_requested'
WORKFLOW_SCHEDULED = 'workflow_scheduled'
WORKFLOW_DELAYED = 'workflow_delayed'
WORKFLOW_RUNNING = 'workflow_running'
WORKFLOW_PAUSING = 'workflow_pausing'
WORKFLOW_PAUSED = 'workflow_paused'
WORKFLOW_RESUMING = 'workflow_resuming'
WORKFLOW_SUCCEEDED = 'workflow_succeeded'
WORKFLOW_FAILED = 'workflow_failed'
WORKFLOW_CANCELING = 'workflow_canceling'
WORKFLOW_CANCELED = 'workflow_canceled'

# Events for task execution.
TASK_REQUESTED = 'task_requested'
TASK_SCHEDULED = 'task_scheduled'
TASK_DELAYED = 'task_delayed'
TASK_RUNNING = 'task_running'
TASK_PENDING = 'task_pending'
TASK_PAUSING = 'task_pausing'
TASK_PAUSED = 'task_paused'
TASK_RESUMING = 'task_resuming'
TASK_SUCCEEDED = 'task_succeeded'
TASK_FAILED = 'task_failed'
TASK_EXPIRED = 'task_timeout'
TASK_ABANDONED = 'task_abandoned'
TASK_CANCELING = 'task_canceling'
TASK_CANCELED = 'task_canceled'

# Events for action executions.
ACTION_REQUESTED = 'action_requested'
ACTION_SCHEDULED = 'action_scheduled'
ACTION_DELAYED = 'action_delayed'
ACTION_RUNNING = 'action_running'
ACTION_PENDING = 'action_pending'
ACTION_PAUSING = 'action_pausing'
ACTION_PAUSED = 'action_paused'
ACTION_RESUMING = 'action_resuming'
ACTION_SUCCEEDED = 'action_succeeded'
ACTION_FAILED = 'action_failed'
ACTION_EXPIRED = 'action_timeout'
ACTION_ABANDONED = 'action_abandoned'
ACTION_CANCELING = 'action_canceling'
ACTION_CANCELED = 'action_canceled'

ACTION_EXECUTION_EVENTS = [
    ACTION_REQUESTED,
    ACTION_SCHEDULED,
    ACTION_DELAYED,
    ACTION_RUNNING,
    ACTION_PENDING,
    ACTION_PAUSING,
    ACTION_PAUSED,
    ACTION_RESUMING,
    ACTION_SUCCEEDED,
    ACTION_FAILED,
    ACTION_EXPIRED,
    ACTION_ABANDONED,
    ACTION_CANCELING,
    ACTION_CANCELED
]

# Events for special workflow engine operations.
TASK_NOOP_REQUESTED = 'task_noop_requested'
TASK_FAIL_REQUESTED = 'task_fail_requested'

ENGINE_OPERATION_EVENTS = [
    TASK_NOOP_REQUESTED,
    TASK_FAIL_REQUESTED
]


class ExecutionEvent(object):

    def __init__(self, name, state, result=None):
        if not states.is_valid(state):
            raise exc.InvalidState(state)

        self.name = name
        self.state = state
        self.result = result


class ActionExecutionEvent(ExecutionEvent):

    def __init__(self, state, result=None):
        super(ActionExecutionEvent, self).__init__('action_%s' % state, state, result=result)


class TaskNoopEvent(ExecutionEvent):

    def __init__(self):
        self.name = TASK_NOOP_REQUESTED
        self.state = states.SUCCEEDED
        self.result = None


class TaskFailEvent(ExecutionEvent):

    def __init__(self):
        self.name = TASK_FAIL_REQUESTED
        self.state = states.FAILED
        self.result = None
