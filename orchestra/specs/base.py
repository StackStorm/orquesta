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

import copy
import logging

from orchestra import utils
from orchestra.specs import types


LOG = logging.getLogger(__name__)


class BaseSpec(object):
    _version = '1.0'

    _schema = {
        'type': 'object'
    }

    _meta_schema = {
        'type': 'object',
        'properties': {
            'name': types.NONEMPTY_STRING,
            'version': types.VERSION,
            'description': types.NONEMPTY_STRING,
            'tags': types.UNIQUE_STRING_LIST
        },
        'required': ['name', 'version']
    }

    @classmethod
    def merge_schema(cls, source1, source2, overwrite=True):
        properties = utils.merge_dicts(
            copy.deepcopy(source1.get('properties', {})),
            copy.deepcopy(source2.get('properties', {})),
            overwrite=overwrite
        )

        required = list(
            set(copy.deepcopy(source1.get('required', []))).union(
                set(copy.deepcopy(source2.get('required', []))))
        )

        additional = (
            source1.get('additionalProperties', True) and
            source2.get('additionalProperties', True)
        )

        schema = {
            'type': 'object',
            'properties': properties,
            'required': sorted(required)
        }

        if not additional:
            schema['additionalProperties'] = additional

        return schema

    @classmethod
    def get_meta_schema(cls):
        meta_schema = {}

        bases = [b for b in cls.__bases__ if issubclass(b, BaseSpec)]

        for base_cls in bases:
            parent_meta_schema = base_cls.get_meta_schema()
            meta_schema = cls.merge_schema(meta_schema, parent_meta_schema)

        return cls.merge_schema(meta_schema, cls._meta_schema)

    @classmethod
    def get_schema(cls, includes=['meta']):
        schema = {}

        bases = [b for b in cls.__bases__ if issubclass(b, BaseSpec)]

        for base_cls in bases:
            parent_schema = base_cls.get_schema(includes=None)
            schema = cls.merge_schema(schema, parent_schema)

        schema = cls.merge_schema(schema, cls._schema)

        if includes and 'meta' in includes:
            meta_schema = cls.get_meta_schema()
            schema = cls.merge_schema(schema, meta_schema)

        return schema
