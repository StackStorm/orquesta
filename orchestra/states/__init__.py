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

STARTING_STATES = [
    REQUESTED,
    SCHEDULED,
    DELAYED,
    RUNNING,
    PENDING
]

RUNNING_STATES = [
    REQUESTED,
    SCHEDULED,
    DELAYED,
    RUNNING,
    RESUMING
]

ACTIVE_STATES = [
    REQUESTED,
    SCHEDULED,
    DELAYED,
    RUNNING,
    RESUMING,
    PAUSING,
    CANCELING
]

PAUSE_STATES = [
    PAUSING,
    PAUSED
]

CANCEL_STATES = [
    CANCELING,
    CANCELED
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


def is_valid(state):
    return (state is None or state in ALL_STATES)
