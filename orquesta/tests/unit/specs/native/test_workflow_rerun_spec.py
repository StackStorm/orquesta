# Copyright 2021 The StackStorm Authors.
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
from orquesta import requests
from orquesta.tests.unit.specs.native import base as test_base


class TaskRerunRequestTest(test_base.OrchestraWorkflowSpecTest):
    def test_load_task_rerun_request_dict_minimal(self):
        rerun_spec = {"task_id": "task1"}
        rerun_req = requests.TaskRerunRequest(rerun_spec)

        self.assertEqual(rerun_req.task_id, rerun_spec["task_id"])
        self.assertEqual(rerun_req.route, 0)
        self.assertEqual(rerun_req.reset_items, False)
        self.assertEqual(
            rerun_req.task_state_entry_id,
            constants.TASK_STATE_ROUTE_FORMAT % ("task1", "0"),
        )

    def test_new_task_rerun_request(self):
        rerun_req = requests.TaskRerunRequest.new("task1", route=1, reset_items=True)

        self.assertEqual(rerun_req.task_id, "task1")
        self.assertEqual(rerun_req.route, 1)
        self.assertEqual(rerun_req.reset_items, True)
        self.assertEqual(
            rerun_req.task_state_entry_id,
            constants.TASK_STATE_ROUTE_FORMAT % ("task1", "1"),
        )
