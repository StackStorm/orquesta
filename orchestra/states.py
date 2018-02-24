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


LOG = logging.getLogger(__name__)


REQUESTED = 'requested'
SCHEDULED = 'scheduled'
DELAYED = 'delayed'
RUNNING = 'running'
PENDING = 'pending'
PAUSING = 'pausing'
PAUSED = 'paused'
RESUMING = 'resuming'
SUCCEEDED = 'succeeded'
FAILED = 'failed'
EXPIRED = 'timeout'
ABANDONED = 'abandoned'
CANCELING = 'canceling'
CANCELED = 'canceled'
UNSET = 'null'


ALL_STATES = [
    REQUESTED,
    SCHEDULED,
    DELAYED,
    RUNNING,
    PENDING,
    PAUSING,
    PAUSED,
    RESUMING,
    SUCCEEDED,
    FAILED,
    EXPIRED,
    ABANDONED,
    CANCELING,
    CANCELED,
    UNSET
]

ACTIVE_STATES = [
    REQUESTED,
    SCHEDULED,
    DELAYED,
    RUNNING,
    PENDING,
    RESUMING,
]

ABENDED_STATES = [
    FAILED,
    EXPIRED,
    ABANDONED
]

COMPLETED_STATES = [
    SUCCEEDED,
    FAILED,
    EXPIRED,
    ABANDONED,
    CANCELED
]

PAUSE_STATES = [
    PAUSING,
    PAUSED
]

CANCEL_STATES = [
    CANCELING,
    CANCELED
]


VALID_STATE_TRANSITION_MAP = {
    UNSET: [REQUESTED, DELAYED, SCHEDULED, RUNNING],
    REQUESTED: [SCHEDULED, DELAYED, RUNNING] + PAUSE_STATES + CANCEL_STATES,
    DELAYED: [SCHEDULED, RUNNING] + PAUSE_STATES + CANCEL_STATES,
    SCHEDULED: [DELAYED, RUNNING] + PAUSE_STATES + CANCEL_STATES,
    RUNNING: COMPLETED_STATES + PAUSE_STATES + CANCEL_STATES,
    PENDING: [RESUMING, RUNNING] + CANCEL_STATES,
    PAUSING: [PAUSED] + CANCEL_STATES,
    PAUSED: [RESUMING, RUNNING] + CANCEL_STATES,
    RESUMING: [RUNNING] + CANCEL_STATES,
    SUCCEEDED: [],
    FAILED: [],
    EXPIRED: [],
    ABANDONED: [],
    CANCELING: [CANCELED],
    CANCELED: []
}


def is_valid(state):
    return (state is None or state in ALL_STATES)


def is_transition_valid(old_state, new_state):
    if old_state is None:
        old_state = 'null'

    if new_state is None:
        new_state = 'null'

    if old_state not in ALL_STATES:
        raise exc.InvalidState(old_state)

    if old_state not in VALID_STATE_TRANSITION_MAP:
        raise exc.InvalidState(old_state)

    if new_state not in ALL_STATES:
        raise exc.InvalidState(new_state)

    if old_state == new_state or new_state in VALID_STATE_TRANSITION_MAP[old_state]:
        return True

    return False
