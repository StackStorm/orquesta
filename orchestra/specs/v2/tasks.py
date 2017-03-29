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
import six

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

    def __init__(self, name, spec):
        super(TaskDefaultsSpec, self).__init__(name, spec)

        self.wait_before = self.spec.get('wait-before', None)
        self.wait_after = self.spec.get('wait-after', None)
        self.pause_before = self.spec.get('pause-before', None)
        self.timeout = self.spec.get('timeout', None)
        self.retry = policies.RetrySpec(None, spec)
        self.concurrency = self.spec.get('concurrency', None)


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

    def __init__(self, name, spec):
        super(TaskSpec, self).__init__(name, spec)

        self.with_items = self.spec.get('with-items', None)
        self.action = self.spec.get('action', None)
        self.workflow = self.spec.get('workflow', None)
        self.input = self.spec.get('input', {})
        self.publish = self.spec.get('publish', {})
        self.wait_before = self.spec.get('wait-before', None)
        self.wait_after = self.spec.get('wait-after', None)
        self.pause_before = self.spec.get('pause-before', None)
        self.timeout = self.spec.get('timeout', None)
        self.retry = policies.RetrySpec(None, spec)
        self.concurrency = self.spec.get('concurrency', None)


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

    def __init__(self, name, spec):
        super(DirectTaskDefaultsSpec, self).__init__(name, spec)

        self.on_complete = self.spec.get('on-complete', [])
        self.on_success = self.spec.get('on-success', [])
        self.on_error = self.spec.get('on-error', [])


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

    def __init__(self, name, spec):
        super(DirectTaskSpec, self).__init__(name, spec)

        self.join = self.spec.get('join', None)
        self.on_complete = self.spec.get('on-complete', [])
        self.on_success = self.spec.get('on-success', [])
        self.on_error = self.spec.get('on-error', [])


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

    def __init__(self, name, spec):
        super(ReverseTaskDefaultsSpec, self).__init__(name, spec)

        self.requires = self.spec.get('requires', [])

        if isinstance(self.requires, six.string_types):
            self.requires = [self.requires]


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

    def __init__(self, name, spec):
        super(ReverseTaskSpec, self).__init__(name, spec)

        self.requires = self.spec.get('requires', [])

        if isinstance(self.requires, six.string_types):
            self.requires = [self.requires]
