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

from orchestra import events
from orchestra import exceptions as exc
from orchestra import states


LOG = logging.getLogger(__name__)


WORKFLOW_STATE_MACHINE_DATA = {
    states.UNSET: {
        events.WORKFLOW_REQUESTED: states.REQUESTED,
        events.WORKFLOW_SCHEDULED: states.SCHEDULED,
        events.WORKFLOW_DELAYED: states.DELAYED,
        events.WORKFLOW_RUNNING: states.RUNNING
    },
    states.REQUESTED: {
        events.WORKFLOW_SCHEDULED: states.SCHEDULED,
        events.WORKFLOW_DELAYED: states.DELAYED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_PAUSING: states.PAUSING,
        events.WORKFLOW_PAUSED: states.PAUSED,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.SCHEDULED: {
        events.WORKFLOW_DELAYED: states.DELAYED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_PAUSING: states.PAUSING,
        events.WORKFLOW_PAUSED: states.PAUSED,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.DELAYED: {
        events.WORKFLOW_SCHEDULED: states.SCHEDULED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_PAUSING: states.PAUSING,
        events.WORKFLOW_PAUSED: states.PAUSED,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.RUNNING: {
        events.WORKFLOW_PAUSING: states.PAUSING,
        events.WORKFLOW_PAUSED: states.PAUSED,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED,
        events.WORKFLOW_SUCCEEDED: states.SUCCEEDED
    },
    states.PAUSING: {
        events.WORKFLOW_PAUSED: states.PAUSED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_RESUMING: states.RESUMING,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.PAUSED: {
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_RESUMING: states.RESUMING,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED
    },
    states.RESUMING: {
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED
    },
    states.CANCELING: {
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.CANCELED: {
    },
    states.SUCCEEDED: {
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.FAILED: {
    }
}


TASK_STATE_MACHINE_DATA = {
    states.UNSET: {
        events.ACTION_REQUESTED: states.REQUESTED,
        events.ACTION_SCHEDULED: states.SCHEDULED,
        events.ACTION_DELAYED: states.DELAYED,
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_PENDING: states.PENDING,
        events.TASK_NOOP_REQUESTED: states.SUCCEEDED,
        events.TASK_FAIL_REQUESTED: states.FAILED
    },
    states.REQUESTED: {
        events.ACTION_SCHEDULED: states.SCHEDULED,
        events.ACTION_DELAYED: states.DELAYED,
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_PENDING: states.PENDING,
        events.ACTION_PAUSING: states.PAUSING,
        events.ACTION_PAUSED: states.PAUSED,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.EXPIRED,
        events.ACTION_ABANDONED: states.ABANDONED
    },
    states.SCHEDULED: {
        events.ACTION_DELAYED: states.DELAYED,
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_PENDING: states.PENDING,
        events.ACTION_PAUSING: states.PAUSING,
        events.ACTION_PAUSED: states.PAUSED,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.EXPIRED,
        events.ACTION_ABANDONED: states.ABANDONED
    },
    states.DELAYED: {
        events.ACTION_SCHEDULED: states.SCHEDULED,
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_PENDING: states.PENDING,
        events.ACTION_PAUSING: states.PAUSING,
        events.ACTION_PAUSED: states.PAUSED,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.EXPIRED,
        events.ACTION_ABANDONED: states.ABANDONED
    },
    states.RUNNING: {
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_PENDING: states.PENDING,
        events.ACTION_PAUSING: states.PAUSING,
        events.ACTION_PAUSED: states.PAUSED,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.EXPIRED,
        events.ACTION_ABANDONED: states.ABANDONED,
        events.ACTION_SUCCEEDED: states.SUCCEEDED
    },
    states.PENDING: {
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.EXPIRED,
        events.ACTION_ABANDONED: states.ABANDONED,
        events.ACTION_SUCCEEDED: states.SUCCEEDED
    },
    states.PAUSING: {
        events.ACTION_PAUSED: states.PAUSED,
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_RESUMING: states.RESUMING,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.EXPIRED,
        events.ACTION_ABANDONED: states.ABANDONED
    },
    states.PAUSED: {
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_RESUMING: states.RESUMING,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
    },
    states.RESUMING: {
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
    },
    states.CANCELING: {
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.EXPIRED,
        events.ACTION_ABANDONED: states.ABANDONED
    },
    states.CANCELED: {
    },
    states.SUCCEEDED: {
    },
    states.FAILED: {
    },
    states.EXPIRED: {
    },
    states.ABANDONED: {
    }
}


class TaskStateMachine(object):

    @classmethod
    def process_event(cls, task_flow_entry, ac_ex_event):

        # Check if event is valid.
        if ac_ex_event.name not in events.ACTION_EXECUTION_EVENTS + events.ENGINE_OPERATION_EVENTS:
            raise exc.InvalidEvent(ac_ex_event.name)

        # Identify current task state.
        current_task_state = task_flow_entry.get('state', states.UNSET)

        if current_task_state is None:
            current_task_state = states.UNSET

        if current_task_state not in states.ALL_STATES:
            raise exc.InvalidState(current_task_state)

        # Identify new task state for the event.
        if current_task_state not in TASK_STATE_MACHINE_DATA:
            raise exc.InvalidTaskStateTransition(current_task_state, ac_ex_event.name)

        if ac_ex_event.name not in TASK_STATE_MACHINE_DATA[current_task_state]:
            raise exc.InvalidTaskStateTransition(current_task_state, ac_ex_event.name)

        new_task_state = TASK_STATE_MACHINE_DATA[current_task_state][ac_ex_event.name]

        # Assign new state to the task flow entry.
        task_flow_entry['state'] = new_task_state

        return task_flow_entry
