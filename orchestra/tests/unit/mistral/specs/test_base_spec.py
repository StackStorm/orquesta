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

import unittest

from orchestra.specs import types
from orchestra.specs.v2 import base


class MockSpec(base.BaseSpec):
    _version = '2.0'

    _schema = {
        'type': 'object',
        'properties': {
            'attr1': types.NONEMPTY_STRING,
            'attr2': types.NONEMPTY_DICT
        },
        'required': ['attr1'],
        'additionalProperties': False
    }


MOCK_SPEC_SCHEMA = {
    'type': 'object',
    'properties': {
        'name': types.NONEMPTY_STRING,
        'version': dict(
            list(types.VERSION.items()) +
            [('enum', ['2.0', 2.0])]
        ),
        'description': types.NONEMPTY_STRING,
        'tags': types.UNIQUE_STRING_LIST,
        'attr1': types.NONEMPTY_STRING,
        'attr2': types.NONEMPTY_DICT
    },
    'required': ['attr1', 'name', 'version'],
    'additionalProperties': False
}


MOCK_SPEC_NO_META_SCHEMA = {
    'type': 'object',
    'properties': {
        'attr1': types.NONEMPTY_STRING,
        'attr2': types.NONEMPTY_DICT
    },
    'required': ['attr1'],
    'additionalProperties': False
}


class BaseSpecTest(unittest.TestCase):

    def test_get_schema(self):
        self.assertDictEqual(
            MOCK_SPEC_SCHEMA,
            MockSpec.get_schema()
        )

    def test_get_schema_no_meta(self):
        self.assertDictEqual(
            MOCK_SPEC_NO_META_SCHEMA,
            MockSpec.get_schema(includes=None)
        )
