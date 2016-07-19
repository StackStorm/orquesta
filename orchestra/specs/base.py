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
import jsonschema
import logging
import yaml

from orchestra import utils
from orchestra.specs import types
from orchestra.utils import plugin


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

    _expr_evaluator = {
        'yaql': plugin.get_module('orchestra.evaluators', 'yaql')
    }

    @classmethod
    def get_expr_evaluator(cls, language):
        return cls._expr_evaluator[language]

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

    @classmethod
    def validate(cls, spec):
        if not isinstance(spec, dict):
            spec = yaml.safe_load(spec)

        errors = {}

        syntax_errors = sorted(
            cls._validate_syntax(spec),
            key=lambda e: e.get('spec_path', None)
        )

        if syntax_errors:
            errors['syntax'] = syntax_errors

        expr_errors = sorted(
            cls._validate_expressions(spec),
            key=lambda e: e.get('spec_path', None)
        )

        if expr_errors:
            errors['expressions'] = expr_errors

        return errors

    @classmethod
    def _validate_syntax(cls, spec):
        validator = jsonschema.Draft4Validator(cls.get_schema())

        return [
            {
                'message': e.message,
                'spec_path': '.'.join(list(e.absolute_path)) or None,
                'schema_path': '.'.join(list(e.absolute_schema_path)) or None
            }
            for e in validator.iter_errors(spec)
        ]

    @classmethod
    def _validate_expressions(cls, spec):
        return None
