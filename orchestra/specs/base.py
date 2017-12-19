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

import collections
import inspect
import json
import jsonschema
import logging
import re
import six
import yaml

from orchestra import exceptions as exc
from orchestra.expressions import base as expr
from orchestra.utils import dictionary as dict_utils
from orchestra.utils import expression as expr_utils
from orchestra.utils import schema as schema_utils
from orchestra.specs import types


LOG = logging.getLogger(__name__)


def isspec(value):
    return inspect.isclass(value) and issubclass(value, Spec)


class Spec(object):
    _catalog = None

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
        }
    }

    _schema_validator = None

    # Put the name of the spec properties in the order of validation.
    _context_evaluation_sequence = []

    # Put the name of the spec properties that are inputs for the context.
    _context_inputs = []

    def __getattr__(self, name):
        """
        Override __getattr__ so we can dynamically map class attributes
        to spec properties. Per documentation, __getattr__ is called by
        __getattribute__ on AttributeError. In this case, the attribute
        does not physically exist on the class and so __getattr__ is
        called which it is overridden here to access the spec dict.
        """
        # Retrieve from spec if attribute is a meta schema property.
        if name in self._meta_schema.get('properties', {}):
            return self.spec.get(name)

        if name.replace('_', '-') in self._meta_schema.get('properties', {}):
            return self.spec.get(name.replace('_', '-'))

        # Retrieve from spec if attribute is a schema property.
        if name in self._schema.get('properties', {}):
            return self.spec.get(name)

        if name.replace('_', '-') in self._schema.get('properties', {}):
            return self.spec.get(name.replace('_', '-'))

        # Retrieve from spec if attribute match a regex pattern in the schema.
        for pattern in self._schema.get('patternProperties', {}).keys():
            if re.match(pattern, name):
                return self.spec.get(name)

        # Use default for all other attributes.
        return self.__getattribute__(name)

    def __init__(self, spec, name=None, member=False):
        # Update the schema to include schema parts from parent classes.
        self._schema = self.get_schema(includes=None, resolve_specs=False)
        self._meta_schema = self.get_meta_schema()

        if not spec:
            raise ValueError('The spec cannot be type of None.')

        self.spec = (
            yaml.safe_load(spec)
            if not isinstance(spec, dict) and not isinstance(spec, list)
            else spec
        )

        if not isinstance(self.spec, dict) and not isinstance(spec, list):
            raise ValueError('The spec is not type of json or yaml.')

        if name:
            self.name = name

        self.member = member

        schema = (
            self._schema if member else
            schema_utils.merge_schema(self.get_meta_schema(), self._schema)
        )

        # Process attributes defined under properties in the schema.
        property_specs = {
            k: v for k, v
            in six.iteritems(schema.get('properties', {}))
            if isspec(v)
        }

        for name, spec_cls in six.iteritems(property_specs):
            if self.spec.get(name):
                setattr(self, name, spec_cls(self.spec.get(name), member=True))

        # Process pattern properties (regex) defined in the schema.
        regex_property_specs = {
            k: v for k, v
            in six.iteritems(schema.get('patternProperties', {}))
            if isspec(v)
        }

        for pattern, spec_cls in six.iteritems(regex_property_specs):
            for name, value in six.iteritems(self.spec):
                if re.match(pattern, name) and value:
                    setattr(self, name, spec_cls(value, member=True))

    @classmethod
    def get_catalog(cls):
        return cls._catalog

    @classmethod
    def get_version(cls):
        return cls._version

    @classmethod
    def get_schema_validator(cls):
        if not cls._schema_validator:
            cls._schema_validator = jsonschema.Draft4Validator(cls.get_schema())

        return cls._schema_validator

    @classmethod
    def get_meta_schema(cls):
        meta_schema = {}

        bases = [b for b in cls.__bases__ if issubclass(b, Spec)]

        for base_cls in bases:
            parent_meta_schema = base_cls.get_meta_schema()
            meta_schema = schema_utils.merge_schema(
                meta_schema,
                parent_meta_schema
            )

        return schema_utils.merge_schema(meta_schema, cls._meta_schema)

    @classmethod
    def get_schema(cls, includes=['meta'], resolve_specs=True):
        schema = {}

        bases = [b for b in cls.__bases__ if issubclass(b, Spec)]

        for base_cls in bases:
            parent_schema = base_cls.get_schema(includes=None)
            schema = schema_utils.merge_schema(schema, parent_schema)

        schema = schema_utils.merge_schema(schema, cls._schema)

        if includes and 'meta' in includes:
            meta_schema = cls.get_meta_schema()
            schema = schema_utils.merge_schema(schema, meta_schema)

        if not resolve_specs:
            return schema

        # Resolve the schema for children specs under properties.
        for k, v in six.iteritems(schema.get('properties', {})):
            if inspect.isclass(v) and issubclass(v, Spec):
                schema['properties'][k] = v.get_schema(includes=None)

        # Resolve the schema for children specs under patternProperties.
        for k, v in six.iteritems(schema.get('patternProperties', {})):
            if inspect.isclass(v) and issubclass(v, Spec):
                schema['patternProperties'][k] = v.get_schema(includes=None)

        return schema

    @classmethod
    def get_expr_schema_paths(cls, schema=None):
        expr_schema_paths = {}

        if not schema:
            schema = cls.get_schema(includes=None)

        for prop_name, prop_type in six.iteritems(schema['properties']):
            base_schema_path = 'properties.' + prop_name

            if isinstance(prop_type, dict) and 'properties' in prop_type:
                sub_paths = cls.get_expr_schema_paths(schema=prop_type)

                for sub_expr_path, sub_schema_path in six.iteritems(sub_paths):
                    expr_path = prop_name + '.' + sub_expr_path
                    schema_path = base_schema_path + '.' + sub_schema_path
                    expr_schema_paths[expr_path] = schema_path
            else:
                expr_schema_paths[prop_name] = base_schema_path

        return expr_schema_paths

    def validate(self):
        errors = {}

        syntax_errors = sorted(
            self._validate_syntax(),
            key=lambda e: e['schema_path']
        )

        if syntax_errors:
            errors['syntax'] = syntax_errors

        expr_errors = sorted(
            self._validate_expressions(),
            key=lambda e: e['schema_path']
        )

        if expr_errors:
            errors['expressions'] = expr_errors

        ctx_errors, _ = self._validate_context()

        if ctx_errors:
            errors['context'] = ctx_errors

        return errors

    def _validate_syntax(self):
        validator = self.get_schema_validator()

        return [
            {
                'message': e.message,
                'spec_path': '.'.join(list(e.absolute_path)) or None,
                'schema_path': '.'.join(list(e.absolute_schema_path)) or None
            }
            for e in validator.iter_errors(self.spec)
        ]

    def _validate_expressions(self):
        result = []
        expr_schema_paths = self.get_expr_schema_paths()

        for expr_path, schema_path in six.iteritems(expr_schema_paths):
            statement = dict_utils.get_dict_value(self.spec, expr_path) or ''
            errors = expr.validate(statement).get('errors', [])

            for error in errors:
                error['spec_path'] = expr_path
                error['schema_path'] = schema_path

            result += errors

        return result

    def _validate_context(self, parent=None):
        errors = []
        parent_ctx = parent.get('ctx', []) if parent else []
        rolling_ctx = list(set(parent_ctx))

        if parent and not parent.get('spec_path', None):
            raise ValueError('Parent context is missing spec path.')

        if parent and not parent.get('schema_path', None):
            raise ValueError('Parent context is missing schema path.')

        def decorate_ctx_var(variable, spec_path, schema_path):
            return {
                'type': variable[0],
                'expression': variable[1],
                'name': variable[2],
                'spec_path': spec_path,
                'schema_path': schema_path
            }

        def decorate_ctx_var_error(var):
            message = (
                'Variable "%s" is referenced before assignment.'
                % var['name']
            )

            error = expr_utils.format_error(
                var['type'],
                var['expression'],
                message,
                var['spec_path'],
                var['schema_path']
            )

            return error

        def get_ctx_inputs(prop_name, prop_value):
            ctx_inputs = []

            # By default, context inputs support only dictionary
            # or list of single item dictionaries.
            if isinstance(prop_value, dict):
                ctx_inputs = list(prop_value.keys())
            elif isinstance(prop_value, list):
                for prop_value_item in prop_value:
                    if isinstance(prop_value_item, six.string_types):
                        ctx_inputs.append(prop_value_item)
                    elif (isinstance(prop_value_item, dict) and
                            len(prop_value_item) == 1):
                        ctx_inputs.extend(list(prop_value_item.keys()))

            return ctx_inputs

        for prop_name in self._context_evaluation_sequence:
            prop_value = getattr(self, prop_name)

            if not prop_value:
                continue

            ctx_vars = []

            spec_path = (
                parent.get('spec_path') + '.' + prop_name
                if parent else self.name + '.' + prop_name
            )

            schema_path = (
                parent.get('schema_path') + '.' + 'properties.' + prop_name
                if parent else 'properties.' + prop_name
            )

            if isinstance(prop_value, Spec):
                parent = {
                    'ctx': rolling_ctx,
                    'spec_path': spec_path,
                    'schema_path': schema_path
                }

                result = prop_value._validate_context(parent=parent)
                errors.extend(result[0])
                rolling_ctx = list(set(rolling_ctx + result[1]))

                continue

            for var in expr.extract_vars(prop_value):
                ctx_vars.append(decorate_ctx_var(var, spec_path, schema_path))

            for ctx_var in ctx_vars:
                if ctx_var['name'] not in rolling_ctx:
                    errors.append(decorate_ctx_var_error(ctx_var))

            if prop_name in self._context_inputs:
                updated_ctx = get_ctx_inputs(prop_name, prop_value)
                rolling_ctx = list(set(rolling_ctx + updated_ctx))

        return (
            sorted(errors, key=lambda x: x['spec_path']),
            rolling_ctx
        )


class MappingSpec(Spec):

    def __setitem__(self, key, item):
        raise NotImplementedError()

    def __getitem__(self, key):
        if key not in self.keys():
            raise KeyError(key)

        return getattr(self, key)

    def __delitem__(self, key):
        raise NotImplementedError()

    def __repr__(self):
        return repr(self.spec)

    def __len__(self):
        return len(self.spec)

    def __cmp__(self, spec):
        return (
            cmp(self.spec, spec)
            if isinstance(spec, dict)
            else cmp(self.spec, spec.spec)
        )

    def __contains__(self, item):
        return item in self.spec

    def __iter__(self):
        for key in self.keys():
            yield (key, getattr(self, key))

    def __unicode__(self):
        return unicode(repr(self.spec))

    def copy(self):
        name = (
            self.name
            if 'name' not in self.spec and hasattr(self, 'name')
            else None
        )

        return self.__class__(
            self.spec.copy(),
            name=name,
            member=self.member
        )

    def keys(self):
        return self.spec.keys()

    def values(self):
        return [getattr(self, key) for key in self.keys()]

    def items(self):
        return zip(self.keys(), self.values())

    def iteritems(self):
        for key in self.keys():
            yield (key, getattr(self, key))

    def clear(self):
        raise NotImplementedError()

    def pop(self, key, default=None):
        raise NotImplementedError()

    def update(self, *args, **kwargs):
        raise NotImplementedError()


class SequenceSpec(Spec, collections.MutableSequence):

    def __init__(self, spec, name=None, member=False):
        super(SequenceSpec, self).__init__(spec, name=name, member=member)

        schema = (
            self._schema if member else
            schema_utils.merge_schema(self.get_meta_schema(), self._schema)
        )

        if schema.get('type') != 'array':
            msg = 'The schema for SequenceSpec must be type of array.'
            raise exc.SchemaDefinitionError(msg)

        if not isspec(schema.get('items')):
            msg = 'The item type for the array must be type of Spec.'
            raise exc.SchemaDefinitionError(msg)

        if not isinstance(self.spec, list):
            raise ValueError('The spec is not type of list.')

        spec_cls = schema['items']
        self._items = [spec_cls(item, member=True) for item in self.spec]

    def __len__(self):
        return len(self._items)

    def __getitem__(self, index):
        return self._items[index]

    def __delitem__(self, index):
        raise NotImplementedError()

    def __setitem__(self, index, item):
        raise NotImplementedError()

    def insert(self, index, item):
        raise NotImplementedError()

    def __str__(self):
        return json.dumps(self.spec, indent=4)
