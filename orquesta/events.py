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

from orquesta import exceptions as exc
from orquesta import states


LOG = logging.getLogger(__name__)


# Events for workflow execution.
WORKFLOW_REQUESTED = 'workflow_requested'
WORKFLOW_SCHEDULED = 'workflow_scheduled'
WORKFLOW_DELAYED = 'workflow_delayed'
WORKFLOW_RUNNING = 'workflow_running'
WORKFLOW_RUNNING_WORKFLOW_COMPLETED = 'workflow_running_workflow_completed'
WORKFLOW_PAUSING = 'workflow_pausing'
WORKFLOW_PAUSING_TASK_ACTIVE_ITEMS_INCOMPLETE = 'workflow_pausing_task_active_items_incomplete'
WORKFLOW_PAUSING_TASK_DORMANT_ITEMS_INCOMPLETE = 'workflow_pausing_task_dormant_items_incomplete'
WORKFLOW_PAUSING_WORKFLOW_ACTIVE = 'workflow_pausing_workflow_active'
WORKFLOW_PAUSING_WORKFLOW_DORMANT = 'workflow_pausing_workflow_dormant'
WORKFLOW_PAUSED = 'workflow_paused'
WORKFLOW_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE = 'workflow_paused_task_active_items_incomplete'
WORKFLOW_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE = 'workflow_paused_task_dormant_items_incomplete'
WORKFLOW_PAUSED_WORKFLOW_ACTIVE = 'workflow_paused_workflow_active'
WORKFLOW_PAUSED_WORKFLOW_DORMANT = 'workflow_paused_workflow_dormant'
WORKFLOW_RESUMING = 'workflow_resuming'
WORKFLOW_RESUMING_WORKFLOW_COMPLETED = 'workflow_resuming_workflow_completed'
WORKFLOW_SUCCEEDED = 'workflow_succeeded'
WORKFLOW_FAILED = 'workflow_failed'
WORKFLOW_CANCELING = 'workflow_canceling'
WORKFLOW_CANCELING_TASK_ACTIVE_ITEMS_INCOMPLETE = 'workflow_canceling_task_active_items_incomplete'
WORKFLOW_CANCELING_TASK_DORMANT_ITEMS_INCOMPLETE = \
    'workflow_canceling_task_dormant_items_incomplete'
WORKFLOW_CANCELING_WORKFLOW_ACTIVE = 'workflow_canceling_workflow_active'
WORKFLOW_CANCELING_WORKFLOW_DORMANT = 'workflow_canceling_workflow_dormant'
WORKFLOW_CANCELED = 'workflow_canceled'
WORKFLOW_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE = 'workflow_canceled_task_active_items_incomplete'
WORKFLOW_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE = \
    'workflow_canceled_task_dormant_items_incomplete'
WORKFLOW_CANCELED_WORKFLOW_ACTIVE = 'workflow_canceled_workflow_active'
WORKFLOW_CANCELED_WORKFLOW_DORMANT = 'workflow_canceled_workflow_dormant'

WORKFLOW_EXECUTION_EVENTS = [
    WORKFLOW_REQUESTED,
    WORKFLOW_SCHEDULED,
    WORKFLOW_DELAYED,
    WORKFLOW_RUNNING,
    WORKFLOW_RUNNING_WORKFLOW_COMPLETED,
    WORKFLOW_PAUSING,
    WORKFLOW_PAUSING_TASK_ACTIVE_ITEMS_INCOMPLETE,
    WORKFLOW_PAUSING_TASK_DORMANT_ITEMS_INCOMPLETE,
    WORKFLOW_PAUSING_WORKFLOW_ACTIVE,
    WORKFLOW_PAUSING_WORKFLOW_DORMANT,
    WORKFLOW_PAUSED,
    WORKFLOW_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    WORKFLOW_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE,
    WORKFLOW_PAUSED_WORKFLOW_ACTIVE,
    WORKFLOW_PAUSED_WORKFLOW_DORMANT,
    WORKFLOW_RESUMING,
    WORKFLOW_RESUMING_WORKFLOW_COMPLETED,
    WORKFLOW_SUCCEEDED,
    WORKFLOW_FAILED,
    WORKFLOW_CANCELING,
    WORKFLOW_CANCELING_TASK_ACTIVE_ITEMS_INCOMPLETE,
    WORKFLOW_CANCELING_TASK_DORMANT_ITEMS_INCOMPLETE,
    WORKFLOW_CANCELING_WORKFLOW_ACTIVE,
    WORKFLOW_CANCELING_WORKFLOW_DORMANT,
    WORKFLOW_CANCELED,
    WORKFLOW_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    WORKFLOW_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE,
    WORKFLOW_CANCELED_WORKFLOW_ACTIVE,
    WORKFLOW_CANCELED_WORKFLOW_DORMANT
]


# Events for task execution.
TASK_REQUESTED = 'task_requested'
TASK_SCHEDULED = 'task_scheduled'
TASK_DELAYED = 'task_delayed'
TASK_RUNNING = 'task_running'
TASK_PENDING = 'task_pending'
TASK_PENDING_WORKFLOW_ACTIVE = 'task_pending_workflow_active'
TASK_PENDING_WORKFLOW_DORMANT = 'task_pending_workflow_dormant'
TASK_PAUSING = 'task_pausing'
TASK_PAUSED = 'task_paused'
TASK_PAUSED_WORKFLOW_ACTIVE = 'task_paused_workflow_active'
TASK_PAUSED_WORKFLOW_DORMANT = 'task_paused_workflow_dormant'
TASK_RESUMING = 'task_resuming'
TASK_SUCCEEDED = 'task_succeeded'
TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE = 'task_succeeded_workflow_active_incomplete'
TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED = 'task_succeeded_workflow_active_completed'
TASK_SUCCEEDED_WORKFLOW_ACTIVE_PAUSED = 'task_succeeded_workflow_active_paused'
TASK_SUCCEEDED_WORKFLOW_ACTIVE_CANCELED = 'task_succeeded_workflow_active_canceled'
TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE = 'task_succeeded_workflow_dormant_incomplete'
TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED = 'task_succeeded_workflow_dormant_completed'
TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED = 'task_succeeded_workflow_dormant_paused'
TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED = 'task_succeeded_workflow_dormant_canceled'
TASK_FAILED = 'task_failed'
TASK_FAILED_WORKFLOW_ACTIVE = 'task_failed_workflow_active'
TASK_FAILED_WORKFLOW_DORMANT = 'task_failed_workflow_dormant'
TASK_REMEDIATED = 'task_remediated'
TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE = 'task_remediated_workflow_active_incomplete'
TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED = 'task_remediated_workflow_active_completed'
TASK_REMEDIATED_WORKFLOW_ACTIVE_PAUSED = 'task_remediated_workflow_active_paused'
TASK_REMEDIATED_WORKFLOW_ACTIVE_CANCELED = 'task_remediated_workflow_active_canceled'
TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE = 'task_remediated_workflow_dormant_incomplete'
TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED = 'task_remediated_workflow_dormant_completed'
TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED = 'task_remediated_workflow_dormant_paused'
TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED = 'task_remediated_workflow_dormant_canceled'
TASK_CANCELING = 'task_canceling'
TASK_CANCELED = 'task_canceled'
TASK_CANCELED_WORKFLOW_ACTIVE = 'task_canceled_workflow_active'
TASK_CANCELED_WORKFLOW_DORMANT = 'task_canceled_workflow_dormant'

# Task events that require additional workflow context for processing.
# These are events related to the tasks being complete or inactive.
TASK_CONDITIONAL_EVENTS = [
    TASK_PENDING,
    TASK_PAUSED,
    TASK_SUCCEEDED,
    TASK_FAILED,
    TASK_REMEDIATED,
    TASK_CANCELED
]

TASK_EXECUTION_EVENTS = [
    TASK_REQUESTED,
    TASK_SCHEDULED,
    TASK_DELAYED,
    TASK_RUNNING,
    TASK_PENDING,
    TASK_PENDING_WORKFLOW_ACTIVE,
    TASK_PENDING_WORKFLOW_DORMANT,
    TASK_PAUSING,
    TASK_PAUSED,
    TASK_PAUSED_WORKFLOW_ACTIVE,
    TASK_PAUSED_WORKFLOW_DORMANT,
    TASK_RESUMING,
    TASK_SUCCEEDED,
    TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE,
    TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED,
    TASK_SUCCEEDED_WORKFLOW_ACTIVE_PAUSED,
    TASK_SUCCEEDED_WORKFLOW_ACTIVE_CANCELED,
    TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE,
    TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED,
    TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED,
    TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED,
    TASK_FAILED,
    TASK_FAILED_WORKFLOW_ACTIVE,
    TASK_FAILED_WORKFLOW_DORMANT,
    TASK_REMEDIATED,
    TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE,
    TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED,
    TASK_REMEDIATED_WORKFLOW_ACTIVE_PAUSED,
    TASK_REMEDIATED_WORKFLOW_ACTIVE_CANCELED,
    TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE,
    TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED,
    TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED,
    TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED,
    TASK_CANCELING,
    TASK_CANCELED,
    TASK_CANCELED_WORKFLOW_ACTIVE,
    TASK_CANCELED_WORKFLOW_DORMANT
]


# Events for action executions.
ACTION_REQUESTED = 'action_requested'
ACTION_SCHEDULED = 'action_scheduled'
ACTION_DELAYED = 'action_delayed'
ACTION_RUNNING = 'action_running'
ACTION_PENDING = 'action_pending'
ACTION_PENDING_TASK_ACTIVE_ITEMS_INCOMPLETE = 'action_pending_task_active_items_incomplete'
ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE = 'action_pending_task_dormant_items_incomplete'
ACTION_PAUSING = 'action_pausing'
ACTION_PAUSED = 'action_paused'
ACTION_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE = 'action_paused_task_active_items_incomplete'
ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE = 'action_paused_task_dormant_items_incomplete'
ACTION_RESUMING = 'action_resuming'
ACTION_SUCCEEDED = 'action_succeeded'
ACTION_SUCCEEDED_TASK_ACTIVE_ITEMS_INCOMPLETE = 'action_succeeded_task_active_items_incomplete'
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE = 'action_succeeded_task_dormant_items_incomplete'
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED = 'action_succeeded_task_dormant_items_completed'
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED = 'action_succeeded_task_dormant_items_paused'
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_CANCELED = 'action_succeeded_task_dormant_items_canceled'
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED = 'action_succeeded_task_dormant_items_failed'
ACTION_FAILED = 'action_failed'
ACTION_FAILED_TASK_ACTIVE_ITEMS_INCOMPLETE = 'action_failed_task_active_items_incomplete'
ACTION_FAILED_TASK_DORMANT_ITEMS_INCOMPLETE = 'action_failed_task_dormant_items_incomplete'
ACTION_EXPIRED = 'action_timeout'
ACTION_ABANDONED = 'action_abandoned'
ACTION_CANCELING = 'action_canceling'
ACTION_CANCELED = 'action_canceled'
ACTION_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE = 'action_canceled_task_active_items_incomplete'
ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE = 'action_canceled_task_dormant_items_incomplete'

ACTION_EXECUTION_EVENTS = [
    ACTION_REQUESTED,
    ACTION_SCHEDULED,
    ACTION_DELAYED,
    ACTION_RUNNING,
    ACTION_PENDING,
    ACTION_PENDING_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_PAUSING,
    ACTION_PAUSED,
    ACTION_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_RESUMING,
    ACTION_SUCCEEDED,
    ACTION_SUCCEEDED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED,
    ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED,
    ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_CANCELED,
    ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED,
    ACTION_FAILED,
    ACTION_FAILED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_FAILED_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_EXPIRED,
    ACTION_ABANDONED,
    ACTION_CANCELING,
    ACTION_CANCELED,
    ACTION_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE
]


# Events for special workflow engine operations.
TASK_NOOP_REQUESTED = 'task_noop_requested'
TASK_FAIL_REQUESTED = 'task_fail_requested'

ENGINE_OPERATION_EVENTS = [
    TASK_NOOP_REQUESTED,
    TASK_FAIL_REQUESTED
]


class ExecutionEvent(object):

    def __init__(self, name, state, result=None, context=None):
        if not states.is_valid(state):
            raise exc.InvalidState(state)

        self.name = name
        self.state = state
        self.result = result
        self.context = context


class WorkflowExecutionEvent(ExecutionEvent):

    def __init__(self, state):
        super(WorkflowExecutionEvent, self).__init__('workflow_%s' % state, state)


class TaskExecutionEvent(ExecutionEvent):

    def __init__(self, task_id, state):
        super(TaskExecutionEvent, self).__init__('task_%s' % state, state)
        self.task_id = task_id


class ActionExecutionEvent(ExecutionEvent):

    def __init__(self, state, result=None, context=None):
        super(ActionExecutionEvent, self).__init__(
            'action_%s' % state,
            state,
            result=result,
            context=context
        )


class EngineOperationEvent(ExecutionEvent):
    pass


class TaskNoopEvent(EngineOperationEvent):

    def __init__(self):
        self.name = TASK_NOOP_REQUESTED
        self.state = states.SUCCEEDED
        self.result = None
        self.context = None


class TaskFailEvent(EngineOperationEvent):

    def __init__(self):
        self.name = TASK_FAIL_REQUESTED
        self.state = states.FAILED
        self.result = None
        self.context = None


ENGINE_EVENT_MAP = {
    'noop': TaskNoopEvent,
    'fail': TaskFailEvent
}
