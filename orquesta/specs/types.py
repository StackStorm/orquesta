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

from orquesta import statuses


NONEMPTY_STRING = {"type": "string", "minLength": 1}

UNIQUE_STRING_LIST = {"type": "array", "items": NONEMPTY_STRING, "uniqueItems": True, "minItems": 1}

POSITIVE_INTEGER = {"type": "integer", "minimum": 0}

POSITIVE_NUMBER = {"type": "number", "minimum": 0.0}

VERSION = {"anyOf": [NONEMPTY_STRING, POSITIVE_INTEGER, POSITIVE_NUMBER]}

ANY = {
    "anyOf": [
        {"type": "array"},
        {"type": "boolean"},
        {"type": "integer"},
        {"type": "number"},
        {"type": "object"},
        {"type": "string"},
    ]
}

ANY_NULLABLE = {
    "anyOf": [
        {"type": "null"},
        {"type": "array"},
        {"type": "boolean"},
        {"type": "integer"},
        {"type": "number"},
        {"type": "object"},
        {"type": "string"},
    ]
}

NONEMPTY_DICT = {
    "type": "object",
    "minProperties": 1,
    "patternProperties": {r"^\w+$": ANY_NULLABLE},
}

ONE_KEY_DICT = {
    "type": "object",
    "minProperties": 1,
    "maxProperties": 1,
    "patternProperties": {r"^\w+$": ANY_NULLABLE},
}

STRING_OR_POSITIVE_INTEGER = {"oneOf": [NONEMPTY_STRING, POSITIVE_INTEGER]}

STRING_OR_POSITIVE_NUMBER = {"oneOf": [NONEMPTY_STRING, POSITIVE_NUMBER]}

STRING_OR_BOOLEAN = {"oneOf": [NONEMPTY_STRING, {"type": "boolean"}]}

STRING_OR_ONE_KEY_DICT = {"oneOf": [NONEMPTY_STRING, ONE_KEY_DICT]}

UNIQUE_STRING_OR_ONE_KEY_DICT_LIST = {
    "type": "array",
    "items": STRING_OR_ONE_KEY_DICT,
    "uniqueItems": True,
    "minItems": 1,
}

UNIQUE_ONE_KEY_DICT_LIST = {
    "type": "array",
    "items": ONE_KEY_DICT,
    "uniqueItems": True,
    "minItems": 1,
}

WORKFLOW_STATUSES = {"type": "string", "enum": statuses.ALL_STATUSES}
