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

REQUESTED = "requested"
SCHEDULED = "scheduled"
DELAYED = "delayed"
RUNNING = "running"
PENDING = "pending"
PAUSING = "pausing"
PAUSED = "paused"
RESUMING = "resuming"
SUCCEEDED = "succeeded"
FAILED = "failed"
EXPIRED = "timeout"
ABANDONED = "abandoned"
RETRYING = "retrying"
CANCELING = "canceling"
CANCELED = "canceled"
UNSET = "null"


ALL_STATUSES = [
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
    RETRYING,
    CANCELING,
    CANCELED,
    UNSET,
]

STARTING_STATUSES = [REQUESTED, SCHEDULED, DELAYED, RUNNING, PENDING]

RUNNING_STATUSES = [REQUESTED, SCHEDULED, DELAYED, RUNNING, RESUMING, RETRYING]

ACTIVE_STATUSES = [REQUESTED, SCHEDULED, DELAYED, RUNNING, RESUMING, PAUSING, CANCELING]

PAUSE_STATUSES = [PAUSING, PAUSED]

CANCEL_STATUSES = [CANCELING, CANCELED]

ABENDED_STATUSES = [FAILED, EXPIRED, ABANDONED]

COMPLETED_STATUSES = [SUCCEEDED, FAILED, EXPIRED, ABANDONED, CANCELED]


def is_valid(status):
    return status is None or status in ALL_STATUSES
