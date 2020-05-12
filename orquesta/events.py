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

import logging

from orquesta import exceptions as exc
from orquesta import statuses


LOG = logging.getLogger(__name__)


# Events for workflow execution.
WORKFLOW_REQUESTED = "workflow_requested"
WORKFLOW_SCHEDULED = "workflow_scheduled"
WORKFLOW_DELAYED = "workflow_delayed"
WORKFLOW_RUNNING = "workflow_running"
WORKFLOW_RUNNING_WORKFLOW_COMPLETED = "workflow_running_workflow_completed"
WORKFLOW_PAUSING = "workflow_pausing"
WORKFLOW_PAUSING_TASK_ACTIVE_ITEMS_INCOMPLETE = "workflow_pausing_task_active_items_incomplete"
WORKFLOW_PAUSING_TASK_DORMANT_ITEMS_INCOMPLETE = "workflow_pausing_task_dormant_items_incomplete"
WORKFLOW_PAUSING_WORKFLOW_ACTIVE = "workflow_pausing_workflow_active"
WORKFLOW_PAUSING_WORKFLOW_DORMANT = "workflow_pausing_workflow_dormant"
WORKFLOW_PAUSED = "workflow_paused"
WORKFLOW_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE = "workflow_paused_task_active_items_incomplete"
WORKFLOW_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE = "workflow_paused_task_dormant_items_incomplete"
WORKFLOW_PAUSED_WORKFLOW_ACTIVE = "workflow_paused_workflow_active"
WORKFLOW_PAUSED_WORKFLOW_DORMANT = "workflow_paused_workflow_dormant"
WORKFLOW_RESUMING = "workflow_resuming"
WORKFLOW_RESUMING_WORKFLOW_COMPLETED = "workflow_resuming_workflow_completed"
WORKFLOW_SUCCEEDED = "workflow_succeeded"
WORKFLOW_FAILED = "workflow_failed"
WORKFLOW_CANCELING = "workflow_canceling"
WORKFLOW_CANCELING_TASK_ACTIVE_ITEMS_INCOMPLETE = "workflow_canceling_task_active_items_incomplete"
WORKFLOW_CANCELING_TASK_DORMANT_ITEMS_INCOMPLETE = (
    "workflow_canceling_task_dormant_items_incomplete"
)
WORKFLOW_CANCELING_WORKFLOW_ACTIVE = "workflow_canceling_workflow_active"
WORKFLOW_CANCELING_WORKFLOW_DORMANT = "workflow_canceling_workflow_dormant"
WORKFLOW_CANCELED = "workflow_canceled"
WORKFLOW_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE = "workflow_canceled_task_active_items_incomplete"
WORKFLOW_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE = "workflow_canceled_task_dormant_items_incomplete"
WORKFLOW_CANCELED_WORKFLOW_ACTIVE = "workflow_canceled_workflow_active"
WORKFLOW_CANCELED_WORKFLOW_DORMANT = "workflow_canceled_workflow_dormant"

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
    WORKFLOW_CANCELED_WORKFLOW_DORMANT,
]


# Events for task execution.
TASK_REQUESTED = "task_requested"
TASK_SCHEDULED = "task_scheduled"
TASK_DELAYED = "task_delayed"
TASK_RUNNING = "task_running"
TASK_PENDING = "task_pending"
TASK_PENDING_WORKFLOW_ACTIVE = "task_pending_workflow_active"
TASK_PENDING_WORKFLOW_DORMANT = "task_pending_workflow_dormant"
TASK_PAUSING = "task_pausing"
TASK_PAUSED = "task_paused"
TASK_PAUSED_WORKFLOW_ACTIVE = "task_paused_workflow_active"
TASK_PAUSED_WORKFLOW_DORMANT = "task_paused_workflow_dormant"
TASK_RESUMING = "task_resuming"
TASK_SUCCEEDED = "task_succeeded"
TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE = "task_succeeded_workflow_active_incomplete"
TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED = "task_succeeded_workflow_active_completed"
TASK_SUCCEEDED_WORKFLOW_ACTIVE_PAUSED = "task_succeeded_workflow_active_paused"
TASK_SUCCEEDED_WORKFLOW_ACTIVE_CANCELED = "task_succeeded_workflow_active_canceled"
TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE = "task_succeeded_workflow_dormant_incomplete"
TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED = "task_succeeded_workflow_dormant_completed"
TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED = "task_succeeded_workflow_dormant_paused"
TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED = "task_succeeded_workflow_dormant_canceled"
TASK_FAILED = "task_failed"
TASK_FAILED_WORKFLOW_ACTIVE = "task_failed_workflow_active"
TASK_FAILED_WORKFLOW_DORMANT = "task_failed_workflow_dormant"
TASK_REMEDIATED = "task_remediated"
TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE = "task_remediated_workflow_active_incomplete"
TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED = "task_remediated_workflow_active_completed"
TASK_REMEDIATED_WORKFLOW_ACTIVE_PAUSED = "task_remediated_workflow_active_paused"
TASK_REMEDIATED_WORKFLOW_ACTIVE_CANCELED = "task_remediated_workflow_active_canceled"
TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE = "task_remediated_workflow_dormant_incomplete"
TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED = "task_remediated_workflow_dormant_completed"
TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED = "task_remediated_workflow_dormant_paused"
TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED = "task_remediated_workflow_dormant_canceled"
TASK_CANCELING = "task_canceling"
TASK_CANCELED = "task_canceled"
TASK_CANCELED_WORKFLOW_ACTIVE = "task_canceled_workflow_active"
TASK_CANCELED_WORKFLOW_DORMANT = "task_canceled_workflow_dormant"
TASK_RETRYING = "task_retrying"
TASK_RETRYING_WORKFLOW_ACTIVE = "task_retrying_workflow_active"
TASK_RETRYING_WORKFLOW_DORMANT = "task_retrying_workflow_dormant"

# Task events that require additional workflow context for processing.
# These are events related to the tasks being complete or inactive.
TASK_CONDITIONAL_EVENTS = [
    TASK_PENDING,
    TASK_PAUSED,
    TASK_SUCCEEDED,
    TASK_FAILED,
    TASK_REMEDIATED,
    TASK_CANCELED,
    TASK_RETRYING,
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
    TASK_CANCELED_WORKFLOW_DORMANT,
    TASK_RETRYING,
    TASK_RETRYING_WORKFLOW_ACTIVE,
    TASK_RETRYING_WORKFLOW_DORMANT,
]


# Events for action executions.
ACTION_REQUESTED = "action_requested"
ACTION_SCHEDULED = "action_scheduled"
ACTION_DELAYED = "action_delayed"
ACTION_RUNNING = "action_running"
ACTION_PENDING = "action_pending"
ACTION_PENDING_TASK_ACTIVE_ITEMS_INCOMPLETE = "action_pending_task_active_items_incomplete"
ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE = "action_pending_task_dormant_items_incomplete"
ACTION_PENDING_TASK_DORMANT_ITEMS_COMPLETED = "action_pending_task_dormant_items_completed"
ACTION_PENDING_TASK_DORMANT_ITEMS_PAUSED = "action_pending_task_dormant_items_paused"
ACTION_PENDING_TASK_DORMANT_ITEMS_CANCELED = "action_pending_task_dormant_items_canceled"
ACTION_PENDING_TASK_DORMANT_ITEMS_FAILED = "action_pending_task_dormant_items_failed"
ACTION_PAUSING = "action_pausing"
ACTION_PAUSED = "action_paused"
ACTION_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE = "action_paused_task_active_items_incomplete"
ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE = "action_paused_task_dormant_items_incomplete"
ACTION_PAUSED_TASK_DORMANT_ITEMS_COMPLETED = "action_paused_task_dormant_items_completed"
ACTION_PAUSED_TASK_DORMANT_ITEMS_PAUSED = "action_paused_task_dormant_items_paused"
ACTION_PAUSED_TASK_DORMANT_ITEMS_CANCELED = "action_paused_task_dormant_items_canceled"
ACTION_PAUSED_TASK_DORMANT_ITEMS_FAILED = "action_paused_task_dormant_items_failed"
ACTION_RESUMING = "action_resuming"
ACTION_SUCCEEDED = "action_succeeded"
ACTION_SUCCEEDED_TASK_ACTIVE_ITEMS_INCOMPLETE = "action_succeeded_task_active_items_incomplete"
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE = "action_succeeded_task_dormant_items_incomplete"
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED = "action_succeeded_task_dormant_items_completed"
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED = "action_succeeded_task_dormant_items_paused"
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_CANCELED = "action_succeeded_task_dormant_items_canceled"
ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED = "action_succeeded_task_dormant_items_failed"
ACTION_FAILED = "action_failed"
ACTION_FAILED_TASK_ACTIVE_ITEMS_INCOMPLETE = "action_failed_task_active_items_incomplete"
ACTION_FAILED_TASK_DORMANT_ITEMS_INCOMPLETE = "action_failed_task_dormant_items_incomplete"
ACTION_FAILED_TASK_DORMANT_ITEMS_COMPLETED = "action_failed_task_dormant_items_completed"
ACTION_FAILED_TASK_DORMANT_ITEMS_PAUSED = "action_failed_task_dormant_items_paused"
ACTION_FAILED_TASK_DORMANT_ITEMS_CANCELED = "action_failed_task_dormant_items_canceled"
ACTION_FAILED_TASK_DORMANT_ITEMS_FAILED = "action_failed_task_dormant_items_failed"
ACTION_EXPIRED = "action_timeout"
ACTION_EXPIRED_TASK_ACTIVE_ITEMS_INCOMPLETE = "action_timeout_task_active_items_incomplete"
ACTION_EXPIRED_TASK_DORMANT_ITEMS_INCOMPLETE = "action_timeout_task_dormant_items_incomplete"
ACTION_EXPIRED_TASK_DORMANT_ITEMS_COMPLETED = "action_timeout_task_dormant_items_completed"
ACTION_EXPIRED_TASK_DORMANT_ITEMS_PAUSED = "action_timeout_task_dormant_items_paused"
ACTION_EXPIRED_TASK_DORMANT_ITEMS_CANCELED = "action_timeout_task_dormant_items_canceled"
ACTION_EXPIRED_TASK_DORMANT_ITEMS_FAILED = "action_timeout_task_dormant_items_failed"
ACTION_ABANDONED = "action_abandoned"
ACTION_ABANDONED_TASK_ACTIVE_ITEMS_INCOMPLETE = "action_abandoned_task_active_items_incomplete"
ACTION_ABANDONED_TASK_DORMANT_ITEMS_INCOMPLETE = "action_abandoned_task_dormant_items_incomplete"
ACTION_ABANDONED_TASK_DORMANT_ITEMS_COMPLETED = "action_abandoned_task_dormant_items_completed"
ACTION_ABANDONED_TASK_DORMANT_ITEMS_PAUSED = "action_abandoned_task_dormant_items_paused"
ACTION_ABANDONED_TASK_DORMANT_ITEMS_CANCELED = "action_abandoned_task_dormant_items_canceled"
ACTION_ABANDONED_TASK_DORMANT_ITEMS_FAILED = "action_abandoned_task_dormant_items_failed"
ACTION_CANCELING = "action_canceling"
ACTION_CANCELED = "action_canceled"
ACTION_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE = "action_canceled_task_active_items_incomplete"
ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE = "action_canceled_task_dormant_items_incomplete"
ACTION_CANCELED_TASK_DORMANT_ITEMS_COMPLETED = "action_canceled_task_dormant_items_completed"
ACTION_CANCELED_TASK_DORMANT_ITEMS_PAUSED = "action_canceled_task_dormant_items_paused"
ACTION_CANCELED_TASK_DORMANT_ITEMS_CANCELED = "action_canceled_task_dormant_items_canceled"
ACTION_CANCELED_TASK_DORMANT_ITEMS_FAILED = "action_canceled_task_dormant_items_failed"

ACTION_EXECUTION_EVENTS = [
    ACTION_REQUESTED,
    ACTION_SCHEDULED,
    ACTION_DELAYED,
    ACTION_RUNNING,
    ACTION_PENDING,
    ACTION_PENDING_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_PENDING_TASK_DORMANT_ITEMS_COMPLETED,
    ACTION_PENDING_TASK_DORMANT_ITEMS_PAUSED,
    ACTION_PENDING_TASK_DORMANT_ITEMS_CANCELED,
    ACTION_PENDING_TASK_DORMANT_ITEMS_FAILED,
    ACTION_PAUSING,
    ACTION_PAUSED,
    ACTION_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_PAUSED_TASK_DORMANT_ITEMS_COMPLETED,
    ACTION_PAUSED_TASK_DORMANT_ITEMS_PAUSED,
    ACTION_PAUSED_TASK_DORMANT_ITEMS_CANCELED,
    ACTION_PAUSED_TASK_DORMANT_ITEMS_FAILED,
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
    ACTION_FAILED_TASK_DORMANT_ITEMS_COMPLETED,
    ACTION_FAILED_TASK_DORMANT_ITEMS_PAUSED,
    ACTION_FAILED_TASK_DORMANT_ITEMS_CANCELED,
    ACTION_FAILED_TASK_DORMANT_ITEMS_FAILED,
    ACTION_EXPIRED,
    ACTION_EXPIRED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_EXPIRED_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_EXPIRED_TASK_DORMANT_ITEMS_COMPLETED,
    ACTION_EXPIRED_TASK_DORMANT_ITEMS_PAUSED,
    ACTION_EXPIRED_TASK_DORMANT_ITEMS_CANCELED,
    ACTION_EXPIRED_TASK_DORMANT_ITEMS_FAILED,
    ACTION_ABANDONED,
    ACTION_ABANDONED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_ABANDONED_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_ABANDONED_TASK_DORMANT_ITEMS_COMPLETED,
    ACTION_ABANDONED_TASK_DORMANT_ITEMS_PAUSED,
    ACTION_ABANDONED_TASK_DORMANT_ITEMS_CANCELED,
    ACTION_ABANDONED_TASK_DORMANT_ITEMS_FAILED,
    ACTION_CANCELING,
    ACTION_CANCELED,
    ACTION_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE,
    ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE,
    ACTION_CANCELED_TASK_DORMANT_ITEMS_COMPLETED,
    ACTION_CANCELED_TASK_DORMANT_ITEMS_PAUSED,
    ACTION_CANCELED_TASK_DORMANT_ITEMS_CANCELED,
    ACTION_CANCELED_TASK_DORMANT_ITEMS_FAILED,
]


# Events for special workflow engine operations.
TASK_CONTINUE_REQUESTED = "task_continue_requested"
TASK_FAIL_REQUESTED = "task_fail_requested"
TASK_NOOP_REQUESTED = "task_noop_requested"
TASK_RETRY_REQUESTED = "task_retry_requested"

ENGINE_OPERATION_EVENTS = [
    TASK_CONTINUE_REQUESTED,
    TASK_FAIL_REQUESTED,
    TASK_NOOP_REQUESTED,
    TASK_RETRY_REQUESTED,
]


class ExecutionEvent(object):
    def __init__(self, name, status, result=None, context=None):
        if not statuses.is_valid(status):
            raise exc.InvalidStatus(status)

        self.name = name
        self.status = status
        self.result = result
        self.context = context


class WorkflowExecutionEvent(ExecutionEvent):
    def __init__(self, status):
        super(WorkflowExecutionEvent, self).__init__("workflow_%s" % status, status)


class TaskExecutionEvent(ExecutionEvent):
    def __init__(self, task_id, route, status):
        super(TaskExecutionEvent, self).__init__("task_%s" % status, status)
        self.task_id = task_id
        self.route = route


class ActionExecutionEvent(ExecutionEvent):
    def __init__(self, status, result=None, context=None):
        super(ActionExecutionEvent, self).__init__(
            "action_%s" % status, status, result=result, context=context
        )


class TaskItemActionExecutionEvent(ActionExecutionEvent):
    def __init__(self, item_id, status, result=None, accumulated_result=None):
        super(TaskItemActionExecutionEvent, self).__init__(status, result=result)
        self.item_id = item_id
        self.accumulated_result = accumulated_result


class EngineOperationEvent(ExecutionEvent):
    pass


class TaskContinueEvent(EngineOperationEvent):
    def __init__(self):
        self.name = TASK_CONTINUE_REQUESTED
        self.status = statuses.SUCCEEDED
        self.result = None
        self.context = None


class TaskRetryEvent(EngineOperationEvent):
    def __init__(self):
        self.name = TASK_RETRY_REQUESTED
        self.status = statuses.RETRYING
        self.result = None
        self.context = None


class TaskNoopEvent(EngineOperationEvent):
    def __init__(self):
        self.name = TASK_NOOP_REQUESTED
        self.status = statuses.SUCCEEDED
        self.result = None
        self.context = None


class TaskFailEvent(EngineOperationEvent):
    def __init__(self):
        self.name = TASK_FAIL_REQUESTED
        self.status = statuses.FAILED
        self.result = None
        self.context = None


ENGINE_EVENT_MAP = {
    "continue": TaskContinueEvent,
    "fail": TaskFailEvent,
    "noop": TaskNoopEvent,
    "retry": TaskRetryEvent,
}
