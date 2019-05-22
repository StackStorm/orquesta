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

from orquesta.specs.mistral.v2 import base as mistral_v2_spec_base
from orquesta.specs import types as spec_types


LOG = logging.getLogger(__name__)


KEEP_RESULT_SCHEMA = spec_types.STRING_OR_BOOLEAN
WAIT_BEFORE_SCHEMA = spec_types.STRING_OR_POSITIVE_INTEGER
WAIT_AFTER_SCHEMA = spec_types.STRING_OR_POSITIVE_INTEGER
TIMEOUT_SCHEMA = spec_types.STRING_OR_POSITIVE_INTEGER
PAUSE_BEFORE_SCHEMA = spec_types.STRING_OR_BOOLEAN
CONCURRENCY_SCHEMA = spec_types.STRING_OR_POSITIVE_INTEGER
SAFE_RERUN_SCHEMA = spec_types.STRING_OR_BOOLEAN
TARGET_SCHEMA = spec_types.NONEMPTY_STRING


class RetrySpec(mistral_v2_spec_base.Spec):
    _schema = {
        'type': 'object',
        'properties': {
            'count': spec_types.STRING_OR_POSITIVE_INTEGER,
            'break-on': spec_types.NONEMPTY_STRING,
            'continue-on': spec_types.NONEMPTY_STRING,
            'delay': spec_types.STRING_OR_POSITIVE_INTEGER
        },
        'required': ['delay', 'count'],
        'additionalProperties': False
    }


class PoliciesSpec(mistral_v2_spec_base.Spec):
    _schema = {
        'type': 'object',
        'properties': {
            'retry': RetrySpec,
            'wait-before': WAIT_BEFORE_SCHEMA,
            'wait-after': WAIT_AFTER_SCHEMA,
            'timeout': TIMEOUT_SCHEMA,
            'pause-before': PAUSE_BEFORE_SCHEMA,
            'concurrency': CONCURRENCY_SCHEMA,
        },
        'additionalProperties': False
    }
