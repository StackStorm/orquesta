# Copyright 2021 The StackStorm Authors.
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

from orquesta import constants
from orquesta.specs.native.v1 import base as native_v1_specs
from orquesta.specs import types as spec_types


class TaskRerunRequest(native_v1_specs.Spec):
    _schema = {
        "type": "object",
        "properties": {
            "task_id": spec_types.NONEMPTY_STRING,
            "route": {"type": "integer", "minimum": 0, "default": 0},
            "reset_items": {"type": "boolean", "default": False},
        },
        "additionalProperties": False,
        "required": ["task_id"],
    }

    def __init__(self, spec, name=None, member=False):
        super(TaskRerunRequest, self).__init__(spec, name=name, member=member)

        self.task_state_entry_id = constants.TASK_STATE_ROUTE_FORMAT % (
            self.task_id,
            str(self.route),
        )

    @classmethod
    def new(cls, task_id, route=0, reset_items=False):
        return cls({"task_id": task_id, "route": route, "reset_items": reset_items})


class TaskRerunRequestSequenceSpec(native_v1_specs.SequenceSpec):
    _schema = {"type": "array", "items": TaskRerunRequest, "default": []}
