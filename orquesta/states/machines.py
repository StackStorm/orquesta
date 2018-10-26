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

from orquesta import events
from orquesta import exceptions as exc
from orquesta import states


LOG = logging.getLogger(__name__)


WORKFLOW_STATE_MACHINE_DATA = {
    states.UNSET: {
        events.WORKFLOW_REQUESTED: states.REQUESTED,
        events.WORKFLOW_SCHEDULED: states.SCHEDULED,
        events.WORKFLOW_DELAYED: states.DELAYED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.REQUESTED: {
        events.WORKFLOW_SCHEDULED: states.SCHEDULED,
        events.WORKFLOW_DELAYED: states.DELAYED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.SCHEDULED: {
        events.WORKFLOW_DELAYED: states.DELAYED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.DELAYED: {
        events.WORKFLOW_SCHEDULED: states.SCHEDULED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_FAILED: states.FAILED
    },
    states.RUNNING: {
        events.WORKFLOW_PAUSING_WORKFLOW_ACTIVE: states.PAUSING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_ACTIVE: states.PAUSING,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_ACTIVE: states.CANCELING,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_ACTIVE: states.CANCELING,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: states.CANCELED,
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
        events.WORKFLOW_PAUSING_WORKFLOW_ACTIVE: states.PAUSING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_ACTIVE: states.PAUSING,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_ACTIVE: states.CANCELING,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_ACTIVE: states.CANCELING,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_RESUMING: states.RESUMING,
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
        events.WORKFLOW_RUNNING_WORKFLOW_COMPLETED: states.SUCCEEDED,
        events.WORKFLOW_RESUMING: states.RESUMING,
        events.WORKFLOW_RESUMING_WORKFLOW_COMPLETED: states.SUCCEEDED,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: states.CANCELED,
        events.TASK_RUNNING: states.RUNNING,
        events.TASK_RESUMING: states.RUNNING
    },
    states.RESUMING: {
        events.WORKFLOW_PAUSING_WORKFLOW_ACTIVE: states.PAUSING,
        events.WORKFLOW_PAUSING_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_PAUSED_WORKFLOW_ACTIVE: states.PAUSING,
        events.WORKFLOW_PAUSED_WORKFLOW_DORMANT: states.PAUSED,
        events.WORKFLOW_CANCELING_WORKFLOW_ACTIVE: states.CANCELING,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_ACTIVE: states.CANCELING,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_RUNNING: states.RUNNING,
        events.WORKFLOW_FAILED: states.FAILED,
        events.TASK_RUNNING: states.RUNNING,
        events.TASK_PENDING_WORKFLOW_ACTIVE: states.PAUSING,
        events.TASK_PENDING_WORKFLOW_DORMANT: states.PAUSED,
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
        events.WORKFLOW_CANCELING_WORKFLOW_ACTIVE: states.CANCELING,
        events.WORKFLOW_CANCELING_WORKFLOW_DORMANT: states.CANCELED,
        events.WORKFLOW_CANCELED_WORKFLOW_ACTIVE: states.CANCELING,
        events.WORKFLOW_CANCELED_WORKFLOW_DORMANT: states.CANCELED,
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
        events.ACTION_PENDING_TASK_ACTIVE_ITEMS_INCOMPLETE: states.PAUSING,
        events.ACTION_PENDING_TASK_DORMANT_ITEMS_INCOMPLETE: states.PAUSED,
        events.ACTION_PAUSING: states.PAUSING,
        events.ACTION_PAUSED: states.PAUSED,
        events.ACTION_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE: states.PAUSING,
        events.ACTION_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE: states.PAUSED,
        events.ACTION_CANCELING: states.CANCELING,
        events.ACTION_CANCELED: states.CANCELED,
        events.ACTION_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE: states.CANCELING,
        events.ACTION_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE: states.CANCELED,
        events.ACTION_FAILED: states.FAILED,
        events.ACTION_FAILED_TASK_ACTIVE_ITEMS_INCOMPLETE: states.RUNNING,
        events.ACTION_FAILED_TASK_DORMANT_ITEMS_INCOMPLETE: states.FAILED,
        events.ACTION_EXPIRED: states.FAILED,
        events.ACTION_ABANDONED: states.FAILED,
        events.ACTION_SUCCEEDED: states.SUCCEEDED,
        events.ACTION_SUCCEEDED_TASK_ACTIVE_ITEMS_INCOMPLETE: states.RUNNING,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE: states.RUNNING,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED: states.PAUSED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_FAILED: states.FAILED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED: states.SUCCEEDED,
        events.WORKFLOW_PAUSING_TASK_ACTIVE_ITEMS_INCOMPLETE: states.PAUSING,
        events.WORKFLOW_PAUSING_TASK_DORMANT_ITEMS_INCOMPLETE: states.PAUSED,
        events.WORKFLOW_PAUSED_TASK_ACTIVE_ITEMS_INCOMPLETE: states.PAUSING,
        events.WORKFLOW_PAUSED_TASK_DORMANT_ITEMS_INCOMPLETE: states.PAUSED,
        events.WORKFLOW_CANCELING_TASK_ACTIVE_ITEMS_INCOMPLETE: states.CANCELING,
        events.WORKFLOW_CANCELING_TASK_DORMANT_ITEMS_INCOMPLETE: states.CANCELED,
        events.WORKFLOW_CANCELED_TASK_ACTIVE_ITEMS_INCOMPLETE: states.CANCELING,
        events.WORKFLOW_CANCELED_TASK_DORMANT_ITEMS_INCOMPLETE: states.CANCELED
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
        events.ACTION_ABANDONED: states.FAILED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_PAUSED: states.PAUSED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE: states.PAUSED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED: states.SUCCEEDED
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
        events.ACTION_ABANDONED: states.FAILED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_CANCELED: states.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_INCOMPLETE: states.CANCELED,
        events.ACTION_SUCCEEDED_TASK_DORMANT_ITEMS_COMPLETED: states.SUCCEEDED
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
    def add_context_to_action_event(cls, conductor, task_id, ac_ex_event):
        action_event = ac_ex_event.name

        requirements = [
            states.PENDING,
            states.PAUSED,
            states.SUCCEEDED,
            states.FAILED,
            states.CANCELED
        ]

        if (ac_ex_event.state in requirements and
                ac_ex_event.context and 'item_id' in ac_ex_event.context):
            # Make a copy of the items and remove current item under evaluation.
            items = copy.deepcopy(conductor.flow.staged[task_id]['items'])
            del items[ac_ex_event.context['item_id']]
            items_status = [item['state'] for item in items]

            # Assess various situations.
            active = list(filter(lambda x: x in states.ACTIVE_STATES, items_status))
            incomplete = list(filter(lambda x: x not in states.COMPLETED_STATES, items_status))
            paused = list(filter(lambda x: x in [states.PENDING, states.PAUSED], items_status))
            canceled = list(filter(lambda x: x == states.CANCELED, items_status))
            failed = list(filter(lambda x: x in states.ABENDED_STATES, items_status))

            # Attach contextual information with the event.
            action_event += '_task_active' if active else '_task_dormant'

            if not active and (paused or canceled or failed):
                if paused:
                    action_event += '_items_paused'

                if canceled:
                    action_event += '_items_canceled'

                if failed:
                    action_event += '_items_failed'

                return action_event

            action_event += '_items_incomplete' if incomplete else '_items_completed'
            return action_event

        return action_event

    @classmethod
    def process_action_event(cls, conductor, task_flow_entry, ac_ex_event):
        # Check if event is valid.
        if ac_ex_event.name not in events.ACTION_EXECUTION_EVENTS + events.ENGINE_OPERATION_EVENTS:
            raise exc.InvalidEvent(ac_ex_event.name)

        # Append additional task context to the event.
        task_id = task_flow_entry['id']
        event_name = cls.add_context_to_action_event(conductor, task_id, ac_ex_event)

        # Identify current task state.
        current_task_state = task_flow_entry.get('state', states.UNSET)

        if current_task_state is None:
            current_task_state = states.UNSET

        if current_task_state not in states.ALL_STATES:
            raise exc.InvalidState(current_task_state)

        if current_task_state not in TASK_STATE_MACHINE_DATA:
            raise exc.InvalidTaskStateTransition(current_task_state, event_name)

        # If no transition is identified, then there is no state change.
        if event_name not in TASK_STATE_MACHINE_DATA[current_task_state]:
            return

        new_task_state = TASK_STATE_MACHINE_DATA[current_task_state][event_name]

        # Assign new state to the task flow entry.
        task_flow_entry['state'] = new_task_state

    @classmethod
    def add_context_to_workflow_event(cls, conductor, task_id, wf_ex_event):
        workflow_event = wf_ex_event.name
        requirements = states.PAUSE_STATES + states.CANCEL_STATES

        task_with_items = (
            task_id in conductor.flow.staged and
            'items' in conductor.flow.staged[task_id]
        )

        if wf_ex_event.state in requirements and task_with_items:
            items_status = [item['state'] for item in conductor.flow.staged[task_id]['items']]
            active = list(filter(lambda x: x in states.ACTIVE_STATES, items_status))
            incomplete = list(filter(lambda x: x not in states.COMPLETED_STATES, items_status))
            workflow_event += '_task_active' if active else '_task_dormant'
            workflow_event += '_items_incomplete' if incomplete else '_items_completed'

        return workflow_event

    @classmethod
    def process_workflow_event(cls, conductor, task_flow_entry, wf_ex_event):
        # Check if event is valid.
        if wf_ex_event.name not in events.WORKFLOW_EXECUTION_EVENTS:
            raise exc.InvalidEvent(wf_ex_event.name)

        # Append additional task context to the event.
        task_id = task_flow_entry['id']
        event_name = cls.add_context_to_workflow_event(conductor, task_id, wf_ex_event)

        # Identify current task state.
        current_task_state = task_flow_entry.get('state', states.UNSET)

        if current_task_state is None:
            current_task_state = states.UNSET

        if current_task_state not in states.ALL_STATES:
            raise exc.InvalidState(current_task_state)

        if current_task_state not in TASK_STATE_MACHINE_DATA:
            raise exc.InvalidTaskStateTransition(current_task_state, event_name)

        # If no transition is identified, then there is no state change.
        if event_name not in TASK_STATE_MACHINE_DATA[current_task_state]:
            return

        new_task_state = TASK_STATE_MACHINE_DATA[current_task_state][event_name]

        # Assign new state to the task flow entry.
        task_flow_entry['state'] = new_task_state

    @classmethod
    def process_event(cls, conductor, task_flow_entry, event):
        if isinstance(event, events.WorkflowExecutionEvent):
            cls.process_workflow_event(conductor, task_flow_entry, event)
            return

        if isinstance(event, events.ActionExecutionEvent):
            cls.process_action_event(conductor, task_flow_entry, event)
            return

        if isinstance(event, events.EngineOperationEvent):
            cls.process_action_event(conductor, task_flow_entry, event)
            return

        raise exc.InvalidEventType(type(event), event.name)


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
    def add_context_to_task_event(cls, conductor, tk_ex_event):
        # Identify current workflow state.
        task_event = tk_ex_event.name
        has_next_tasks = conductor.has_next_tasks(getattr(tk_ex_event, 'task_id', None))
        has_active_tasks = conductor.flow.has_active_tasks

        # Mark task remediated if task is in abended states and there are transitions.
        if tk_ex_event.state in states.ABENDED_STATES and has_next_tasks:
            task_event = events.TASK_REMEDIATED

        # For certain events like cancel and pause, whether there are tasks in active
        # state determine whether the workflow reached final state or still in progress.
        # For example, if the workflow is being canceled and there are other active
        # tasks, the workflow should be set to canceling.
        if task_event in events.TASK_CONDITIONAL_EVENTS:
            task_event += '_workflow_active' if has_active_tasks else '_workflow_dormant'

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

        if conductor.flow.has_staged_tasks or has_next_tasks:
            return task_event + '_incomplete'

        return task_event + '_completed'

    @classmethod
    def process_task_event(cls, conductor, tk_ex_event):
        # Append additional workflow context to the event.
        event_name = cls.add_context_to_task_event(conductor, tk_ex_event)

        # Check if event is valid.
        if event_name not in events.TASK_EXECUTION_EVENTS:
            raise exc.InvalidEvent(event_name)

        # Capture current workflow state.
        current_workflow_state = conductor.get_workflow_state()
        new_workflow_state = current_workflow_state

        # Check if the current workflow state can be transitioned.
        if current_workflow_state not in WORKFLOW_STATE_MACHINE_DATA:
            raise exc.InvalidWorkflowStateTransition(current_workflow_state, event_name)

        # If the current workflow state can be transitioned and there is no match on the
        # event, then there is not state transition.
        if event_name not in WORKFLOW_STATE_MACHINE_DATA[current_workflow_state]:
            return

        new_workflow_state = WORKFLOW_STATE_MACHINE_DATA[current_workflow_state][event_name]

        # Assign new workflow state if there is change.
        if current_workflow_state != new_workflow_state:
            conductor._set_workflow_state(new_workflow_state)

    @classmethod
    def add_context_to_workflow_event(cls, conductor, wf_ex_event):
        # Identify current workflow state.
        workflow_event = wf_ex_event.name
        has_active_tasks = conductor.flow.has_active_tasks

        # For certain events like cancel and pause, whether there are tasks in active
        # state determine whether the workflow reached final state or still in progress.
        # For example, if the workflow is being canceled and there are other active
        # tasks, the workflow should be set to canceling.
        if wf_ex_event.state in states.PAUSE_STATES + states.CANCEL_STATES:
            workflow_event += '_workflow_active' if has_active_tasks else '_workflow_dormant'

        # If the workflow is paused and on resume, check whether it is already completed.
        if (conductor.get_workflow_state() == states.PAUSED and
                wf_ex_event.state in [states.RUNNING, states.RESUMING] and
                not conductor.flow.has_active_tasks and
                not conductor.flow.has_staged_tasks and
                not conductor.flow.has_paused_tasks):
            workflow_event += '_workflow_completed'

        return workflow_event

    @classmethod
    def process_workflow_event(cls, conductor, wf_ex_event):
        # Append additional workflow context to the event.
        event_name = cls.add_context_to_workflow_event(conductor, wf_ex_event)

        # Check if event is valid.
        if event_name not in events.WORKFLOW_EXECUTION_EVENTS:
            raise exc.InvalidEvent(event_name)

        # Capture current workflow state.
        current_workflow_state = conductor.get_workflow_state()
        new_workflow_state = current_workflow_state

        # Check if the current workflow state can be transitioned.
        if current_workflow_state not in WORKFLOW_STATE_MACHINE_DATA:
            raise exc.InvalidWorkflowStateTransition(current_workflow_state, event_name)

        # If the current workflow state can be transitioned and there is no match on the
        # event, then there is not state transition.
        if event_name not in WORKFLOW_STATE_MACHINE_DATA[current_workflow_state]:
            return

        new_workflow_state = WORKFLOW_STATE_MACHINE_DATA[current_workflow_state][event_name]

        # Assign new workflow state if there is change.
        if current_workflow_state != new_workflow_state:
            conductor._set_workflow_state(new_workflow_state)

    @classmethod
    def process_event(cls, conductor, event):
        if isinstance(event, events.WorkflowExecutionEvent):
            cls.process_workflow_event(conductor, event)
            return

        if isinstance(event, events.TaskExecutionEvent):
            cls.process_task_event(conductor, event)
            return

        raise exc.InvalidEventType(type(event), event.name)
