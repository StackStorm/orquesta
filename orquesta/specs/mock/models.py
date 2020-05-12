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

from orquesta.specs import base as spec_base


LOG = logging.getLogger(__name__)


def instantiate(definition):
    if len(definition.keys()) > 1:
        raise ValueError("Workflow definition contains more than one workflow.")

    wf_name, wf_spec = list(definition.items())[0]

    return WorkflowSpec(wf_spec, name=wf_name)


def deserialize(data):
    return WorkflowSpec.deserialize(data)


class WorkflowSpec(spec_base.Spec):
    _catalog = "mock"
