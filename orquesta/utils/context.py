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

from orquesta.utils import jsonify as json_util


LOG = logging.getLogger(__name__)


def set_current_task(context, task):
    if context and not isinstance(context, dict):
        raise TypeError("The context is not type of dict.")

    if not task:
        raise ValueError("The task is not set.")

    if not isinstance(task, dict):
        raise TypeError("The task is not type of dict.")

    ctx = json_util.deepcopy(context) if context else dict()

    ctx["__current_task"] = {"id": task.get("id"), "route": task.get("route")}

    if "result" in task:
        ctx["__current_task"]["result"] = task.get("result")

    return ctx


def set_current_item(context, item):
    if context and not isinstance(context, dict):
        raise TypeError("The context is not type of dict.")

    ctx = json_util.deepcopy(context) if context else dict()
    ctx["__current_item"] = item

    return ctx
