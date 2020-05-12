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

from orquesta import events
from orquesta import exceptions as exc
from orquesta import statuses
from orquesta.utils import jsonify as json_util


LOG = logging.getLogger(__name__)


WORKFLOW_STATE_MACHINE_DATA = {
    statuses.UNSET: {
        events.WORKFLOW_REQUESTED: statuses.REQUESTED,
        events.WORKFLOW_SCHEDULED: statuses.SCHEDULED,
        events.WORKFLOW_DELAYED: statuses.DELAYED,
        events.WORKFLOW_RUNNING: statuses.RUNNING,
        events.WORKFLOW_FAILED: statuses.FAILED,
    },
    statuses.REQUESTED: {
        events.WORKFLOW_SCHEDULED: statuses.SCHEDULED,
        events.WORKFLOW_DELAYED: statuses.DELAYED,
        events.WORKFLOW_RUNNING: statuses.RUNNING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_FAILED: statuses.FAILED,
    },
    statuses.SCHEDULED: {
        events.WORKFLOW_DELAYED: statuses.DELAYED,
        events.WORKFLOW_RUNNING: statuses.RUNNING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_FAILED: statuses.FAILED,
    },
    statuses.DELAYED: {
        events.WORKFLOW_SCHEDULED: statuses.SCHEDULED,
        events.WORKFLOW_RUNNING: statuses.RUNNING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_FAILED: statuses.FAILED,
    },
    statuses.RUNNING: {
        events.WORKFLOW_PAUSING_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_FAILED: statuses.FAILED,
        events.WORKFLOW_SUCCEEDED: statuses.SUCCEEDED,
        events.TASK_PENDING_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.TASK_PENDING_WORKFLOW_DORMANT: statuses.PAUSED,
        # Workflow status is not affected by task going into pause statuses. This allows for more
        # granular control if operators want to pause a task but keep the rest of running.
        events.TASK_PAUSING: statuses.RUNNING,
        events.TASK_PAUSED_WORKFLOW_ACTIVE: statuses.RUNNING,
        # But if the workflow is not active, then pause the workflow when the last task paused.
        events.TASK_PAUSED_WORKFLOW_DORMANT: statuses.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE: statuses.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED: statuses.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_PAUSED: statuses.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_CANCELED: statuses.CANCELING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE: statuses.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED: statuses.SUCCEEDED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED: statuses.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED: statuses.CANCELED,
        # Fail the workflow quickly even if there are still running tasks.
        events.TASK_FAILED_WORKFLOW_ACTIVE: statuses.FAILED,
        events.TASK_FAILED_WORKFLOW_DORMANT: statuses.FAILED,
        # A task is remediated if it failed but there is one or more task transitions.
        # Therefore, remediated tasks are treated as if they are succeeded tasks.
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE: statuses.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED: statuses.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_PAUSED: statuses.PAUSING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_CANCELED: statuses.CANCELING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE: statuses.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED: statuses.SUCCEEDED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED: statuses.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED: statuses.CANCELED,
        events.TASK_CANCELING: statuses.CANCELING,
        events.TASK_CANCELED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.TASK_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
    },
    statuses.PAUSING: {
        events.WORKFLOW_PAUSING_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_RUNNING: statuses.RUNNING,
        events.WORKFLOW_RESUMING: statuses.RESUMING,
        events.WORKFLOW_FAILED: statuses.FAILED,
        events.TASK_PENDING_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.TASK_PENDING_WORKFLOW_DORMANT: statuses.PAUSED,
        events.TASK_PAUSED_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.TASK_PAUSED_WORKFLOW_DORMANT: statuses.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE: statuses.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED: statuses.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_PAUSED: statuses.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_CANCELED: statuses.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE: statuses.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED: statuses.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED: statuses.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED: statuses.CANCELED,
        events.TASK_FAILED_WORKFLOW_ACTIVE: statuses.FAILED,
        events.TASK_FAILED_WORKFLOW_DORMANT: statuses.FAILED,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE: statuses.PAUSING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED: statuses.PAUSING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_PAUSED: statuses.PAUSING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_CANCELED: statuses.CANCELING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE: statuses.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED: statuses.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED: statuses.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED: statuses.CANCELED,
        events.TASK_CANCELING: statuses.CANCELING,
        events.TASK_CANCELED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.TASK_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.TASK_RETRYING_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.TASK_RETRYING_WORKFLOW_DORMANT: statuses.PAUSED,
    },
    statuses.PAUSED: {
        events.WORKFLOW_RUNNING: statuses.RUNNING,
        events.WORKFLOW_RUNNING_WORKFLOW_COMPLETED: statuses.SUCCEEDED,
        events.WORKFLOW_RESUMING: statuses.RESUMING,
        events.WORKFLOW_RESUMING_WORKFLOW_COMPLETED: statuses.SUCCEEDED,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_FAILED: statuses.FAILED,
        events.TASK_RUNNING: statuses.RUNNING,
        events.TASK_RESUMING: statuses.RUNNING,
    },
    statuses.RESUMING: {
        events.WORKFLOW_PAUSING_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: statuses.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_RUNNING: statuses.RUNNING,
        events.WORKFLOW_FAILED: statuses.FAILED,
        events.TASK_RUNNING: statuses.RUNNING,
        events.TASK_PENDING_WORKFLOW_ACTIVE: statuses.PAUSING,
        events.TASK_PENDING_WORKFLOW_DORMANT: statuses.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE: statuses.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED: statuses.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE: statuses.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED: statuses.SUCCEEDED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED: statuses.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED: statuses.CANCELED,
        events.TASK_FAILED_WORKFLOW_ACTIVE: statuses.FAILED,
        events.TASK_FAILED_WORKFLOW_DORMANT: statuses.FAILED,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE: statuses.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED: statuses.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE: statuses.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED: statuses.SUCCEEDED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED: statuses.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED: statuses.CANCELED,
        events.TASK_CANCELING: statuses.CANCELING,
        events.TASK_CANCELED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.TASK_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
    },
    statuses.CANCELING: {
        events.WORKFLOW_CANCELING_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.WORKFLOW_FAILED: statuses.FAILED,
        events.TASK_PENDING_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.TASK_PENDING_WORKFLOW_DORMANT: statuses.CANCELED,
        events.TASK_PAUSED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.TASK_PAUSED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE: statuses.CANCELING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED: statuses.CANCELING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE: statuses.CANCELED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED: statuses.CANCELED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED: statuses.CANCELED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED: statuses.CANCELED,
        events.TASK_FAILED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.TASK_FAILED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE: statuses.CANCELING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED: statuses.CANCELING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE: statuses.CANCELED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED: statuses.CANCELED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED: statuses.CANCELED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED: statuses.CANCELED,
        events.TASK_CANCELED_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.TASK_CANCELED_WORKFLOW_DORMANT: statuses.CANCELED,
        events.TASK_RETRYING_WORKFLOW_ACTIVE: statuses.CANCELING,
        events.TASK_RETRYING_WORKFLOW_DORMANT: statuses.CANCELED,
    },
    statuses.CANCELED: {},
    statuses.SUCCEEDED: {
        # Workflow status can transition from succeeded to failed in  cases
        # where there is exception while rendering workflow output.
        events.WORKFLOW_FAILED: statuses.FAILED
    },
    statuses.FAILED: {},
}


TASK_STATE_MACHINE_DATA = {
    statuses.UNSET: {
        events.ACTION_REQUESTED: statuses.REQUESTED,
        events.ACTION_SCHEDULED: statuses.SCHEDULED,
        events.ACTION_DELAYED: statuses.DELAYED,
        events.ACTION_RUNNING: statuses.RUNNING,
        events.ACTION_PENDING: statuses.PENDING,
        events.TASK_CONTINUE_REQUESTED: statuses.SUCCEEDED,
        events.TASK_NOOP_REQUESTED: statuses.SUCCEEDED,
        events.TASK_FAIL_REQUESTED: statuses.FAILED,
    },
    statuses.REQUESTED: {
        events.ACTION_SCHEDULED: statuses.SCHEDULED,
        events.ACTION_DELAYED: statuses.DELAYED,
        events.ACTION_RUNNING: statuses.RUNNING,
        events.ACTION_PENDING: statuses.PENDING,
        events.ACTION_PAUSING: statuses.PAUSING,
        events.ACTION_PAUSED: statuses.PAUSED,
        events.ACTION_CANCELING: statuses.CANCELING,
        events.ACTION_CANCELED: statuses.CANCELED,
        events.ACTION_FAILED: statuses.FAILED,
        events.ACTION_EXPIRED: statuses.EXPIRED,
        events.ACTION_ABANDONED: statuses.ABANDONED,
    },
    statuses.SCHEDULED: {
        events.ACTION_DELAYED: statuses.DELAYED,
        events.ACTION_RUNNING: statuses.RUNNING,
        events.ACTION_PENDING: statuses.PENDING,
        events.ACTION_PAUSING: statuses.PAUSING,
        events.ACTION_PAUSED: statuses.PAUSED,
        events.ACTION_CANCELING: statuses.CANCELING,
        events.ACTION_CANCELED: statuses.CANCELED,
        events.ACTION_FAILED: statuses.FAILED,
        events.ACTION_EXPIRED: statuses.EXPIRED,
        events.ACTION_ABANDONED: statuses.ABANDONED,
    },
    statuses.DELAYED: {
        events.ACTION_SCHEDULED: statuses.SCHEDULED,
        events.ACTION_RUNNING: statuses.RUNNING,
        events.ACTION_PENDING: statuses.PENDING,
        events.ACTION_PAUSING: statuses.PAUSING,
        events.ACTION_PAUSED: statuses.PAUSED,
        events.ACTION_CANCELING: statuses.CANCELING,
        events.ACTION_CANCELED: statuses.CANCELED,
        events.ACTION_FAILED: statuses.FAILED,
        events.ACTION_EXPIRED: statuses.EXPIRED,
        events.ACTION_ABANDONED: statuses.ABANDONED,
    },
    statuses.RUNNING: {
        events.ACTION_RUNNING: statuses.RUNNING,
        events.ACTION_PENDING: statuses.PENDING,
        events.ACTION_PENDING_TASK_ACTIVE_ITEMS_INCOMPLETE: statuses.PAUSING,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_PAUSED: statuses.PAUSED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.PAUSED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_COMPLETED: statuses.PAUSED,
        events.ACTION_PAUSING: statuses.PAUSING,
        events.ACTION_PAUSED: statuses.PAUSED,
        events.ACTION_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE: statuses.PAUSING,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_PAUSED: statuses.PAUSED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.PAUSED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_COMPLETED: statuses.PAUSED,
        events.ACTION_CANCELING: statuses.CANCELING,
        events.ACTION_CANCELED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE: statuses.CANCELING,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_COMPLETED: statuses.CANCELED,
        events.ACTION_FAILED: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_PAUSED: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_COMPLETED: statuses.FAILED,
        events.ACTION_EXPIRED: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_PAUSED: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_COMPLETED: statuses.FAILED,
        events.ACTION_ABANDONED: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_PAUSED: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_COMPLETED: statuses.FAILED,
        events.ACTION_SUCCEEDED: statuses.SUCCEEDED,
        events.ACTION_SUCCEEDED_TASK_ACTIVE_ITEMS_INCOMPLETE: statuses.RUNNING,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED: statuses.PAUSED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.RUNNING,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED: statuses.SUCCEEDED,
        events.WORKFLOW_PAUSING_TASK_ACTIVE_ITEMS_INCOMPLETE: statuses.PAUSING,
        events.WORKFLOW_PAUSING_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.PAUSED,
        events.WORKFLOW_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE: statuses.PAUSING,
        events.WORKFLOW_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.PAUSED,
        events.WORKFLOW_CANCELING_TASK_ACTIVE_ITEMS_INCOMPLETE: statuses.CANCELING,
        events.WORKFLOW_CANCELING_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.WORKFLOW_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE: statuses.CANCELING,
        events.WORKFLOW_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
    },
    statuses.PENDING: {
        events.ACTION_CANCELED: statuses.CANCELED,
        events.ACTION_FAILED: statuses.FAILED,
        events.ACTION_EXPIRED: statuses.FAILED,
        events.ACTION_ABANDONED: statuses.FAILED,
        events.ACTION_SUCCEEDED: statuses.SUCCEEDED,
    },
    statuses.PAUSING: {
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_PAUSED: statuses.PAUSED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.PAUSED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_COMPLETED: statuses.PAUSED,
        events.ACTION_PAUSED: statuses.PAUSED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_PAUSED: statuses.PAUSED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.PAUSED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_COMPLETED: statuses.PAUSED,
        events.ACTION_RESUMING: statuses.RESUMING,
        events.ACTION_CANCELING: statuses.CANCELING,
        events.ACTION_CANCELED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_COMPLETED: statuses.CANCELED,
        events.ACTION_FAILED: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_PAUSED: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_COMPLETED: statuses.FAILED,
        events.ACTION_EXPIRED: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_PAUSED: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_COMPLETED: statuses.FAILED,
        events.ACTION_ABANDONED: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_PAUSED: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_COMPLETED: statuses.FAILED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED: statuses.PAUSED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED: statuses.FAILED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.PAUSED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED: statuses.SUCCEEDED,
    },
    statuses.PAUSED: {
        events.ACTION_RUNNING: statuses.RUNNING,
        events.ACTION_RESUMING: statuses.RESUMING,
        events.ACTION_CANCELING: statuses.CANCELING,
        events.ACTION_CANCELED: statuses.CANCELED,
    },
    statuses.RESUMING: {
        events.ACTION_RUNNING: statuses.RUNNING,
        events.ACTION_CANCELING: statuses.CANCELING,
        events.ACTION_CANCELED: statuses.CANCELED,
    },
    statuses.RETRYING: {
        events.ACTION_RUNNING: statuses.RUNNING,
        events.ACTION_CANCELING: statuses.CANCELING,
        events.ACTION_CANCELED: statuses.CANCELED,
        events.WORKFLOW_CANCELING: statuses.CANCELED,
        events.WORKFLOW_CANCELED: statuses.CANCELED,
    },
    statuses.CANCELING: {
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_COMPLETED: statuses.CANCELED,
        events.ACTION_PAUSED: statuses.CANCELED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_COMPLETED: statuses.CANCELED,
        events.ACTION_CANCELED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_COMPLETED: statuses.CANCELED,
        events.ACTION_FAILED: statuses.FAILED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_COMPLETED: statuses.CANCELED,
        events.ACTION_EXPIRED: statuses.FAILED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_EXPIRED_TASK_DORMANT_ITEMS_COMPLETED: statuses.CANCELED,
        events.ACTION_ABANDONED: statuses.FAILED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_ABANDONED_TASK_DORMANT_ITEMS_COMPLETED: statuses.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED: statuses.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_CANCELED: statuses.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED: statuses.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE: statuses.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED: statuses.SUCCEEDED,
    },
    statuses.CANCELED: {},
    statuses.SUCCEEDED: {events.TASK_RETRY_REQUESTED: statuses.RETRYING},
    statuses.FAILED: {events.TASK_RETRY_REQUESTED: statuses.RETRYING},
}


class TaskStateMachine(object):
    @classmethod
    def is_transition_valid(cls, old_status, new_status):
        if old_status is None:
            old_status = "null"

        if new_status is None:
            new_status = "null"

        if not statuses.is_valid(old_status):
            raise exc.InvalidStatus(old_status)

        if not statuses.is_valid(new_status):
            raise exc.InvalidStatus(new_status)

        if old_status not in TASK_STATE_MACHINE_DATA.keys():
            return False

        if old_status == new_status or new_status in TASK_STATE_MACHINE_DATA[old_status].values():
            return True

        return False

    @classmethod
    def add_context_to_action_event(cls, workflow_state, task_id, task_route, ac_ex_event):
        return ac_ex_event.name

    @classmethod
    def process_action_event(cls, workflow_state, task_state, ac_ex_event):
        # Check if event is valid.
        if ac_ex_event.name not in events.ACTION_EXECUTION_EVENTS + events.ENGINE_OPERATION_EVENTS:
            raise exc.InvalidEvent(ac_ex_event.name)

        # Append additional task context to the event.
        event_name = cls.add_context_to_action_event(
            workflow_state, task_state["id"], task_state["route"], ac_ex_event
        )

        # Identify current task status.
        current_task_status = task_state.get("status", statuses.UNSET)

        if current_task_status is None:
            current_task_status = statuses.UNSET

        if current_task_status not in statuses.ALL_STATUSES:
            raise exc.InvalidStatus(current_task_status)

        if current_task_status not in TASK_STATE_MACHINE_DATA:
            raise exc.InvalidTaskStatusTransition(current_task_status, event_name)

        # If no transition is identified, then there is no status change.
        if event_name not in TASK_STATE_MACHINE_DATA[current_task_status]:
            return

        new_task_status = TASK_STATE_MACHINE_DATA[current_task_status][event_name]

        # Assign new status to the task flow entry.
        task_state["status"] = new_task_status

    @classmethod
    def add_context_to_task_item_event(cls, workflow_state, task_id, task_route, ac_ex_event):
        action_event = ac_ex_event.name

        requirements = [
            statuses.RESUMING,
            statuses.PENDING,
            statuses.PAUSED,
            statuses.SUCCEEDED,
            statuses.FAILED,
            statuses.EXPIRED,
            statuses.ABANDONED,
            statuses.CANCELED,
        ]

        if ac_ex_event.status in requirements:
            # Make a copy of the items and remove current item under evaluation.
            staged_task = workflow_state.get_staged_task(task_id, task_route)
            items = json_util.deepcopy(staged_task["items"])
            del items[ac_ex_event.item_id]
            items_status = [item.get("status", statuses.UNSET) for item in items]

            # Assess various situations.
            active = list(filter(lambda x: x in statuses.ACTIVE_STATUSES, items_status))
            incomplete = list(filter(lambda x: x not in statuses.COMPLETED_STATUSES, items_status))
            paused = list(filter(lambda x: x in [statuses.PENDING, statuses.PAUSED], items_status))
            canceled = list(filter(lambda x: x == statuses.CANCELED, items_status))
            failed = list(filter(lambda x: x in statuses.ABENDED_STATUSES, items_status))

            # Attach info on whether task is still active or dormant.
            action_event += "_task_active" if active else "_task_dormant"

            # Attach info on whether there are paused execution on the items and return.
            if not active and paused:
                return action_event + "_items_paused"

            # Attach info on whether there are canceled execution on the items and return.
            if not active and canceled:
                return action_event + "_items_canceled"

            # Attach info on whether there are failed execution on the items and return.
            if not active and failed:
                return action_event + "_items_failed"

            # If items are not paused, canceled, or failed, then attach info on whether
            # there are items that are not in one of the completed statuses.
            return action_event + ("_items_incomplete" if incomplete else "_items_completed")

        return action_event

    @classmethod
    def process_task_item_event(cls, workflow_state, task_state, ac_ex_event):
        # Check if event is valid.
        if ac_ex_event.name not in events.ACTION_EXECUTION_EVENTS + events.ENGINE_OPERATION_EVENTS:
            raise exc.InvalidEvent(ac_ex_event.name)

        # Append additional task context to the event.
        event_name = cls.add_context_to_task_item_event(
            workflow_state, task_state["id"], task_state["route"], ac_ex_event
        )

        # Identify current task status.
        current_task_status = task_state.get("status", statuses.UNSET)

        if current_task_status is None:
            current_task_status = statuses.UNSET

        if current_task_status not in statuses.ALL_STATUSES:
            raise exc.InvalidStatus(current_task_status)

        if current_task_status not in TASK_STATE_MACHINE_DATA:
            raise exc.InvalidTaskStatusTransition(current_task_status, event_name)

        # If no transition is identified, then there is no status change.
        if event_name not in TASK_STATE_MACHINE_DATA[current_task_status]:
            return

        new_task_status = TASK_STATE_MACHINE_DATA[current_task_status][event_name]

        # Assign new status to the task flow entry.
        task_state["status"] = new_task_status

    @classmethod
    def add_context_to_workflow_event(cls, workflow_state, task_id, task_route, wf_ex_event):
        workflow_event = wf_ex_event.name
        requirements = statuses.PAUSE_STATUSES + statuses.CANCEL_STATUSES
        staged_task = workflow_state.get_staged_task(task_id, task_route)

        if wf_ex_event.status in requirements and staged_task and "items" in staged_task:
            items_status = [item.get("status", statuses.UNSET) for item in staged_task["items"]]
            active = list(filter(lambda x: x in statuses.ACTIVE_STATUSES, items_status))
            incomplete = list(filter(lambda x: x not in statuses.COMPLETED_STATUSES, items_status))
            workflow_event += "_task_active" if active else "_task_dormant"
            workflow_event += "_items_incomplete" if incomplete else "_items_completed"

        return workflow_event

    @classmethod
    def process_workflow_event(cls, workflow_state, task_state, wf_ex_event):
        # Check if event is valid.
        if wf_ex_event.name not in events.WORKFLOW_EXECUTION_EVENTS:
            raise exc.InvalidEvent(wf_ex_event.name)

        # Append additional task context to the event.
        event_name = cls.add_context_to_workflow_event(
            workflow_state, task_state["id"], task_state["route"], wf_ex_event
        )

        # Identify current task status.
        current_task_status = task_state.get("status", statuses.UNSET)

        if current_task_status is None:
            current_task_status = statuses.UNSET

        if current_task_status not in statuses.ALL_STATUSES:
            raise exc.InvalidStatus(current_task_status)

        if current_task_status not in TASK_STATE_MACHINE_DATA:
            raise exc.InvalidTaskStatusTransition(current_task_status, event_name)

        # If no transition is identified, then there is no status change.
        if event_name not in TASK_STATE_MACHINE_DATA[current_task_status]:
            return

        new_task_status = TASK_STATE_MACHINE_DATA[current_task_status][event_name]

        # Assign new status to the task flow entry.
        task_state["status"] = new_task_status

    @classmethod
    def process_event(cls, workflow_state, task_state, event):
        if isinstance(event, events.WorkflowExecutionEvent):
            cls.process_workflow_event(workflow_state, task_state, event)
            return

        if isinstance(event, events.TaskItemActionExecutionEvent):
            cls.process_task_item_event(workflow_state, task_state, event)
            return

        if isinstance(event, events.ActionExecutionEvent):
            cls.process_action_event(workflow_state, task_state, event)
            return

        if isinstance(event, events.EngineOperationEvent):
            cls.process_action_event(workflow_state, task_state, event)
            return

        raise exc.InvalidEventType(type(event), event.name)


class WorkflowStateMachine(object):
    @classmethod
    def is_transition_valid(cls, old_status, new_status):
        if old_status is None:
            old_status = "null"

        if new_status is None:
            new_status = "null"

        if not statuses.is_valid(old_status):
            raise exc.InvalidStatus(old_status)

        if not statuses.is_valid(new_status):
            raise exc.InvalidStatus(new_status)

        if old_status not in WORKFLOW_STATE_MACHINE_DATA.keys():
            return False

        if (
            old_status == new_status
            or new_status in WORKFLOW_STATE_MACHINE_DATA[old_status].values()
        ):
            return True

        return False

    @classmethod
    def add_context_to_task_event(cls, workflow_state, tk_ex_event):
        # Identify current workflow status.
        task_event = tk_ex_event.name
        task_id = getattr(tk_ex_event, "task_id", None)
        task_route = getattr(tk_ex_event, "route", None)
        has_next_tasks = workflow_state.has_next_tasks(task_id, task_route)
        has_active_tasks = workflow_state.has_active_tasks

        # Mark task remediated if task is in abended statuses and there are transitions.
        if tk_ex_event.status in statuses.ABENDED_STATUSES and has_next_tasks:
            task_event = events.TASK_REMEDIATED

        # For certain events like cancel and pause, whether there are tasks in active
        # status determine whether the workflow reached final status or still in progress.
        # For example, if the workflow is being canceled and there are other active
        # tasks, the workflow should be set to canceling.
        if task_event in events.TASK_CONDITIONAL_EVENTS:
            task_event += "_workflow_active" if has_active_tasks else "_workflow_dormant"

        # When a task succeeded, additional information need to be included in the
        # event to determine whether the workflow is still running, completed, or
        # it needs to be paused or canceled. Cancelation has precedence over other
        # status, follow by pause, running, then completion.

        if not task_event.startswith(events.TASK_SUCCEEDED) and not task_event.startswith(
            events.TASK_REMEDIATED
        ):
            return task_event

        if workflow_state.has_canceling_tasks or workflow_state.has_canceled_tasks:
            return task_event + "_canceled"

        if workflow_state.has_pausing_tasks or workflow_state.has_paused_tasks:
            return task_event + "_paused"

        if workflow_state.has_staged_tasks or has_next_tasks:
            return task_event + "_incomplete"

        return task_event + "_completed"

    @classmethod
    def process_task_event(cls, workflow_state, tk_ex_event):
        # Append additional workflow context to the event.
        event_name = cls.add_context_to_task_event(workflow_state, tk_ex_event)

        # Check if event is valid.
        if event_name not in events.TASK_EXECUTION_EVENTS:
            raise exc.InvalidEvent(event_name)

        # Capture current workflow status.
        current_workflow_status = workflow_state.status
        new_workflow_status = current_workflow_status

        # Check if the current workflow status can be transitioned.
        if current_workflow_status not in WORKFLOW_STATE_MACHINE_DATA:
            raise exc.InvalidWorkflowStatusTransition(current_workflow_status, event_name)

        # If the current workflow status can be transitioned and there is no match on the
        # event, then there is not status transition.
        if event_name not in WORKFLOW_STATE_MACHINE_DATA[current_workflow_status]:
            return

        new_workflow_status = WORKFLOW_STATE_MACHINE_DATA[current_workflow_status][event_name]

        # Assign new workflow status if there is change.
        if current_workflow_status != new_workflow_status:
            workflow_state.status = new_workflow_status

        # If the final workflow status here is completed, then ensure there is no unreachable
        # barrier task(s). A barrier task is unreachable if the workflow is completed but then one
        # or more criteria for the task is satisified. In this case, log the task and fail the
        # workflow to notify that the execution is incomplete but unable to proceed.
        if workflow_state.status in statuses.COMPLETED_STATUSES:
            unreachable_barriers = workflow_state.get_unreachable_barriers()

            # If there are unreachable barrier tasks, then change workflow status to failed
            # and write an error log for each case.
            if unreachable_barriers:
                workflow_state.status = statuses.FAILED

                for entry in unreachable_barriers:
                    e = exc.UnreachableJoinError(entry["id"], entry["route"])
                    workflow_state.conductor.log_error(e, task_id=entry["id"], route=entry["route"])

    @classmethod
    def add_context_to_workflow_event(cls, workflow_state, wf_ex_event):
        # Identify current workflow status.
        workflow_event = wf_ex_event.name
        has_active_tasks = workflow_state.has_active_tasks

        # For certain events like cancel and pause, whether there are tasks in active
        # status determine whether the workflow reached final status or still in progress.
        # For example, if the workflow is being canceled and there are other active
        # tasks, the workflow should be set to canceling.
        if wf_ex_event.status in statuses.PAUSE_STATUSES + statuses.CANCEL_STATUSES:
            workflow_event += "_workflow_active" if has_active_tasks else "_workflow_dormant"

        # If the workflow is paused and on resume, check whether it is already completed.
        if (
            workflow_state.status == statuses.PAUSED
            and wf_ex_event.status in [statuses.RUNNING, statuses.RESUMING]
            and not workflow_state.has_active_tasks
            and not workflow_state.has_staged_tasks
            and not workflow_state.has_paused_tasks
        ):
            workflow_event += "_workflow_completed"

        return workflow_event

    @classmethod
    def process_workflow_event(cls, workflow_state, wf_ex_event):
        # Append additional workflow context to the event.
        event_name = cls.add_context_to_workflow_event(workflow_state, wf_ex_event)

        # Check if event is valid.
        if event_name not in events.WORKFLOW_EXECUTION_EVENTS:
            raise exc.InvalidEvent(event_name)

        # Capture current workflow status.
        current_workflow_status = workflow_state.status
        new_workflow_status = current_workflow_status

        # Check if the current workflow status can be transitioned.
        if current_workflow_status not in WORKFLOW_STATE_MACHINE_DATA:
            raise exc.InvalidWorkflowStatusTransition(current_workflow_status, event_name)

        # If the current workflow status can be transitioned and there is no match on the
        # event, then there is not status transition.
        if event_name not in WORKFLOW_STATE_MACHINE_DATA[current_workflow_status]:
            return

        new_workflow_status = WORKFLOW_STATE_MACHINE_DATA[current_workflow_status][event_name]

        # Assign new workflow status if there is change.
        if current_workflow_status != new_workflow_status:
            workflow_state.status = new_workflow_status

    @classmethod
    def process_event(cls, workflow_state, event):
        if isinstance(event, events.WorkflowExecutionEvent):
            cls.process_workflow_event(workflow_state, event)
            return

        if isinstance(event, events.TaskExecutionEvent):
            cls.process_task_event(workflow_state, event)
            return

        raise exc.InvalidEventType(type(event), event.name)
