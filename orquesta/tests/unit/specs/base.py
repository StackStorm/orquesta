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

from orquesta.specs import base as spec_base
from orquesta.specs import types as spec_types


class MockBaseSpec(spec_base.Spec):
    _catalog = "test"
    _version = "1.0"


class MockBaseMappingSpec(spec_base.MappingSpec):
    _catalog = "test"
    _version = "1.0"


class MockBaseSequenceSpec(spec_base.SequenceSpec):
    _catalog = "test"
    _version = "1.0"


class MockLeafSpec(MockBaseSpec):
    _schema = {
        "type": "object",
        "properties": {"attr1": spec_types.NONEMPTY_STRING, "attr2": spec_types.NONEMPTY_STRING},
        "required": ["attr1"],
        "additionalProperties": False,
    }

    _context_evaluation_sequence = ["attr1", "attr2"]


class MockJointSpec(MockBaseSpec):
    _schema = {
        "type": "object",
        "properties": {"attr1": MockLeafSpec},
        "required": ["attr1"],
        "additionalProperties": False,
    }

    _context_evaluation_sequence = ["attr1"]


class MockMappingSpec(MockBaseMappingSpec):
    _schema = {"type": "object", "patternProperties": {r"^\w+$": MockJointSpec}}


class MockSequenceSpec(MockBaseSequenceSpec):
    _schema = {"type": "array", "items": MockJointSpec}


class MockSpec(MockBaseSpec):
    _schema = {
        "type": "object",
        "properties": {
            "inputs": spec_types.UNIQUE_STRING_OR_ONE_KEY_DICT_LIST,
            "vars": spec_types.NONEMPTY_DICT,
            "attr1": spec_types.NONEMPTY_STRING,
            "attr1-1": spec_types.NONEMPTY_STRING,
            "attr1_2": spec_types.NONEMPTY_STRING,
            "attr2": spec_types.NONEMPTY_DICT,
            "attr3": spec_types.UNIQUE_STRING_LIST,
            "attr4": spec_types.UNIQUE_STRING_OR_ONE_KEY_DICT_LIST,
            "attr5": MockJointSpec,
            "attr6": MockMappingSpec,
            "attr7": MockSequenceSpec,
        },
        "required": ["attr1"],
        "additionalProperties": False,
    }

    _context_evaluation_sequence = [
        "inputs",
        "vars",
        "attr1",
        "attr1-1",
        "attr1_2",
        "attr2",
        "attr3",
        "attr4",
        "attr5",
        "attr6",
        "attr7",
    ]

    _context_inputs = ["inputs", "vars"]
