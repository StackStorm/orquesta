# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from orchestra.specs import types
from orchestra.specs.v2 import base
from orchestra.specs.v2 import policies


LOG = logging.getLogger(__name__)


ON_CLAUSE_SCHEMA = {
    'oneOf': [
        types.NONEMPTY_STRING,
        types.UNIQUE_STRING_OR_YAQL_CONDITION_LIST
    ]
}


class TaskDefaultsSpec(base.BaseSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'retry': policies.RetrySpec.get_schema(includes=None),
            'wait-before': policies.WAIT_BEFORE_SCHEMA,
            'wait-after': policies.WAIT_AFTER_SCHEMA,
            'timeout': policies.TIMEOUT_SCHEMA,
            'pause-before': policies.PAUSE_BEFORE_SCHEMA,
            'concurrency': policies.CONCURRENCY_SCHEMA
        },
        'additionalProperties': False
    }


class TaskSpec(base.BaseSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'type': types.WORKFLOW_TYPE,
            'action': types.NONEMPTY_STRING,
            'workflow': types.NONEMPTY_STRING,
            'input': types.NONEMPTY_DICT,
            'with-items': {
                'oneOf': [
                    types.NONEMPTY_STRING,
                    types.UNIQUE_STRING_LIST
                ]
            },
            'publish': types.NONEMPTY_DICT,
            'retry': policies.RetrySpec.get_schema(includes=None),
            'wait-before': policies.WAIT_BEFORE_SCHEMA,
            'wait-after': policies.WAIT_AFTER_SCHEMA,
            'timeout': policies.TIMEOUT_SCHEMA,
            'pause-before': policies.PAUSE_BEFORE_SCHEMA,
            'concurrency': policies.CONCURRENCY_SCHEMA,
            'target': types.NONEMPTY_STRING,
            'keep-result': types.YAQL_OR_BOOLEAN
        },
        'additionalProperties': False,
        'anyOf': [
            {
                'not': {
                    'type': 'object',
                    'required': ['action', 'workflow']
                },
            },
            {
                'oneOf': [
                    {
                        'type': 'object',
                        'required': ['action']
                    },
                    {
                        'type': 'object',
                        'required': ['workflow']
                    }
                ]
            }
        ]
    }


class DirectTaskDefaultsSpec(TaskDefaultsSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'on-complete': ON_CLAUSE_SCHEMA,
            'on-success': ON_CLAUSE_SCHEMA,
            'on-error': ON_CLAUSE_SCHEMA
        },
        'additionalProperties': False
    }


class DirectTaskSpec(TaskSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'join': {
                'oneOf': [
                    {'enum': ['all']},
                    types.POSITIVE_INTEGER
                ]
            },
            'on-complete': ON_CLAUSE_SCHEMA,
            'on-success': ON_CLAUSE_SCHEMA,
            'on-error': ON_CLAUSE_SCHEMA
        },
        'additionalProperties': False
    }


class ReverseTaskDefaultsSpec(TaskDefaultsSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'requires': {
                'oneOf': [types.NONEMPTY_STRING, types.UNIQUE_STRING_LIST]
            }
        },
        'additionalProperties': False
    }


class ReverseTaskSpec(TaskSpec):
    _schema = {
        'type': 'object',
        'properties': {
            'requires': {
                'oneOf': [types.NONEMPTY_STRING, types.UNIQUE_STRING_LIST]
            }
        },
        'additionalProperties': False
    }
