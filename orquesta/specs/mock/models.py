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
from orquesta.specs import types as spec_types


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


class MappingSpec(spec_base.MappingSpec):
    _catalog = "mock"

    _version = "1.0"

    _meta_schema = {
        "type": "object",
        "properties": {"version": {"enum": [_version, float(_version)]}},
    }


class TestFileSpec(MappingSpec):
    _schema = {
        "type": "object",
        "properties": {
            "workflow": spec_types.NONEMPTY_STRING,
            "routes": {"type": "array", "items": {"type": "array"}},
            "task_sequence": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "task": {"type": "string"},
                        "route": {"type": "integer"},
                        "result": {"type": "object"},
                        "status": {"type": "string"},
                    },
                },
            },
            "inputs": {"type": "object"},
            "expected_workflow_status": {"type": "string"},
            "expected_output": {"type": "object"},
            "expected_term_tasks": {"type": "array"},
        },
        "required": ["workflow", "task_sequence"],
        "additionalProperties": False,
    }

    def __init__(self, spec, name=None, member=False):
        super(TestFileSpec, self).__init__(spec, name=name, member=member)
