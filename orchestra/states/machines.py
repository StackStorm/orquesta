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
        events.WORKFLOW_SUCCEEDED: states.SUCCEEDED,
        events.TASK_PENDING_WORKFLOW_ACTIVE: states.PAUSING,
        events.TASK_PENDING_WORKFLOW_DORMANT: states.PAUSED,
        # Workflow state is not affected by task going into pause states. This allows for more
        # granular control if operators want to pause a task but keep the rest of running.
        events.TASK_PAUSING: states.RUNNING,
        events.TASK_PAUSED_WORKFLOW_ACTIVE: states.RUNNING,
        # But if the workflow is not active, then pause the workflow when the last task paused.
        events.TASK_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE: states.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED: states.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_PAUSED: states.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_CANCELED: states.CANCELING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE: states.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED: states.SUCCEEDED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED: states.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED: states.CANCELED,
        # Fail the workflow quickly even if there are still running tasks.
        events.TASK_FAILED_WORKFLOW_ACTIVE: states.FAILED,
        events.TASK_FAILED_WORKFLOW_DORMANT: states.FAILED,
        # A task is remediated if it failed but there is one or more task transitions.
        # Therefore, remediated tasks are treated as if they are succeeded tasks.
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE: states.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED: states.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_PAUSED: states.PAUSING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_CANCELED: states.CANCELING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE: states.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED: states.SUCCEEDED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED: states.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED: states.CANCELED,
        events.TASK_CANCELING: states.CANCELING,
        events.TASK_CANCELED_WORKFLOW_ACTIVE: states.CANCELING,
        events.TASK_CANCELED_WORKFLOW_DORMANT: states.CANCELED
    },
    states.PAUSING: {
        events.WORKFLOW_PAUSED: states.PAUSED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_RESUMING: states.RESUMING,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED,
        events.TASK_PENDING_WORKFLOW_ACTIVE: states.PAUSING,
        events.TASK_PENDING_WORKFLOW_DORMANT: states.PAUSED,
        events.TASK_PAUSED_WORKFLOW_ACTIVE: states.PAUSING,
        events.TASK_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE: states.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED: states.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_PAUSED: states.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_CANCELED: states.PAUSING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE: states.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED: states.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED: states.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED: states.CANCELED,
        events.TASK_FAILED_WORKFLOW_ACTIVE: states.FAILED,
        events.TASK_FAILED_WORKFLOW_DORMANT: states.FAILED,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE: states.PAUSING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED: states.PAUSING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_PAUSED: states.PAUSING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_CANCELED: states.CANCELING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE: states.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED: states.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED: states.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED: states.CANCELED,
        events.TASK_CANCELING: states.CANCELING,
        events.TASK_CANCELED_WORKFLOW_ACTIVE: states.CANCELING,
        events.TASK_CANCELED_WORKFLOW_DORMANT: states.CANCELED
    },
    states.PAUSED: {
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_RESUMING: states.RESUMING,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.TASK_RUNNING: states.RUNNING,
        events.TASK_RESUMING: states.RUNNING
    },
    states.RESUMING: {
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_PAUSING: states.PAUSING,
        events.WORKFLOW_PAUSED: states.PAUSED,
        events.WORKFLOW_CANCELING: states.CANCELING,
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.TASK_RUNNING: states.RUNNING,
        events.TASK_PENDING_WORKFLOW_ACTIVE: states.PAUSING,
        events.TASK_PENDING_WORKFLOW_DORMANT: states.PAUSED,
        # Workflow in resuming state is almost similar to workflow in running state.
        # Workflow state is not affected by task going into pause states. This allows for more
        # granular control if operators want to pause a task but keep the rest of running.
        events.TASK_PAUSING: states.RUNNING,
        events.TASK_PAUSED_WORKFLOW_ACTIVE: states.RUNNING,
        events.TASK_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE: states.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED: states.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE: states.RUNNING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED: states.SUCCEEDED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED: states.PAUSED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED: states.CANCELED,
        events.TASK_FAILED_WORKFLOW_ACTIVE: states.FAILED,
        events.TASK_FAILED_WORKFLOW_DORMANT: states.FAILED,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE: states.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED: states.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE: states.RUNNING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED: states.SUCCEEDED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED: states.PAUSED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED: states.CANCELED,
        events.TASK_CANCELING: states.CANCELING,
        events.TASK_CANCELED_WORKFLOW_ACTIVE: states.CANCELING,
        events.TASK_CANCELED_WORKFLOW_DORMANT: states.CANCELED
    },
    states.CANCELING: {
        events.WORKFLOW_CANCELED: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED,
        events.TASK_PENDING_WORKFLOW_ACTIVE: states.CANCELING,
        events.TASK_PENDING_WORKFLOW_DORMANT: states.CANCELED,
        events.TASK_PAUSED_WORKFLOW_ACTIVE: states.CANCELING,
        events.TASK_PAUSED_WORKFLOW_DORMANT: states.CANCELED,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_INCOMPLETE: states.CANCELING,
        events.TASK_SUCCEEDED_WORKFLOW_ACTIVE_COMPLETED: states.CANCELING,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_INCOMPLETE: states.CANCELED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_COMPLETED: states.CANCELED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_PAUSED: states.CANCELED,
        events.TASK_SUCCEEDED_WORKFLOW_DORMANT_CANCELED: states.CANCELED,
        events.TASK_FAILED_WORKFLOW_ACTIVE: states.CANCELING,
        events.TASK_FAILED_WORKFLOW_DORMANT: states.CANCELED,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_INCOMPLETE: states.CANCELING,
        events.TASK_REMEDIATED_WORKFLOW_ACTIVE_COMPLETED: states.CANCELING,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_INCOMPLETE: states.CANCELED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_COMPLETED: states.CANCELED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_PAUSED: states.CANCELED,
        events.TASK_REMEDIATED_WORKFLOW_DORMANT_CANCELED: states.CANCELED,
        events.TASK_CANCELED_WORKFLOW_ACTIVE: states.CANCELING,
        events.TASK_CANCELED_WORKFLOW_DORMANT: states.CANCELED
    },
    states.CANCELED: {
    },
    states.SUCCEEDED: {
        # Workflow state can transition from succeeded to failed in  cases
        # where there is exception while rendering workflow output.
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
        events.ACTION_EXPIRED: states.FAILED,
        events.ACTION_ABANDONED: states.FAILED,
        events.ACTION_SUCCEEDED: states.SUCCEEDED
    },
    states.PENDING: {
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.FAILED,
        events.ACTION_ABANDONED: states.FAILED,
        events.ACTION_SUCCEEDED: states.SUCCEEDED
    },
    states.PAUSING: {
        events.ACTION_PAUSED: states.PAUSED,
        events.ACTION_RUNNING: states.RUNNING,
        events.ACTION_RESUMING: states.RESUMING,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_EXPIRED: states.FAILED,
        events.ACTION_ABANDONED: states.FAILED
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
        events.ACTION_EXPIRED: states.FAILED,
        events.ACTION_ABANDONED: states.FAILED
    },
    states.CANCELED: {
    },
    states.SUCCEEDED: {
    },
    states.FAILED: {
    }
}


class TaskStateMachine(object):

    @classmethod
    def is_transition_valid(cls, old_state, new_state):
        if old_state is None:
            old_state = 'null'

        if new_state is None:
            new_state = 'null'

        if not states.is_valid(old_state):
            raise exc.InvalidState(old_state)

        if not states.is_valid(new_state):
            raise exc.InvalidState(new_state)

        if old_state not in TASK_STATE_MACHINE_DATA.keys():
            return False

        if old_state == new_state or new_state in TASK_STATE_MACHINE_DATA[old_state].values():
            return True

        return False

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


class WorkflowStateMachine(object):

    @classmethod
    def is_transition_valid(cls, old_state, new_state):
        if old_state is None:
            old_state = 'null'

        if new_state is None:
            new_state = 'null'

        if not states.is_valid(old_state):
            raise exc.InvalidState(old_state)

        if not states.is_valid(new_state):
            raise exc.InvalidState(new_state)

        if old_state not in WORKFLOW_STATE_MACHINE_DATA.keys():
            return False

        if old_state == new_state or new_state in WORKFLOW_STATE_MACHINE_DATA[old_state].values():
            return True

        return False

    @classmethod
    def add_context_to_event(cls, conductor, tk_ex_event):
        # Identify current workflow state.
        task_id = tk_ex_event.task_id
        task_event = tk_ex_event.name
        task_has_transitions = conductor.has_next_tasks(task_id)
        wf_active = conductor.flow.has_active_tasks

        # Mark task remediated if task is in abended states and there are transitions.
        if tk_ex_event.state in states.ABENDED_STATES and task_has_transitions:
            task_event = events.TASK_REMEDIATED

        # For certain events like cancel and pause, whether there are tasks in active
        # state determine whether the workflow reached final state or still in progress.
        # For example, if the workflow is being canceled and there are other active
        # tasks, the workflow should be set to canceling.
        if task_event in events.TASK_CONDITIONAL_EVENTS:
            task_event += '_workflow_active' if wf_active else '_workflow_dormant'

        # When a task succeeded, additional information need to be included in the
        # event to determine whether the workflow is still running, completed, or
        # it needs to be paused or canceled. Cancelation has precedence over other
        # state, follow by pause, running, then completion.

        if (not task_event.startswith(events.TASK_SUCCEEDED) and
                not task_event.startswith(events.TASK_REMEDIATED)):
            return task_event

        if conductor.flow.has_canceling_tasks or conductor.flow.has_canceled_tasks:
            return task_event + '_canceled'

        if conductor.flow.has_pausing_tasks or conductor.flow.has_paused_tasks:
            return task_event + '_paused'

        if conductor.flow.has_staged_tasks or task_has_transitions:
            return task_event + '_incomplete'

        return task_event + '_completed'

    @classmethod
    def process_event(cls, conductor, tk_ex_event):
        # Append additional workflow context to the event..
        tk_ex_event.name = cls.add_context_to_event(conductor, tk_ex_event)

        # Capture current workflow state.
        current_workflow_state = conductor.get_workflow_state()
        new_workflow_state = current_workflow_state

        # Check if the current workflow state can be transitioned.
        if current_workflow_state not in WORKFLOW_STATE_MACHINE_DATA:
            raise exc.InvalidWorkflowStateTransition(current_workflow_state, tk_ex_event.name)

        # If the current workflow state can be transitioned and there is no match on the
        # event, then there is not state transition.
        if tk_ex_event.name not in WORKFLOW_STATE_MACHINE_DATA[current_workflow_state]:
            return

        new_workflow_state = WORKFLOW_STATE_MACHINE_DATA[current_workflow_state][tk_ex_event.name]

        # Assign new workflow state if there is change.
        if current_workflow_state != new_workflow_state:
            conductor.set_workflow_state(new_workflow_state)
