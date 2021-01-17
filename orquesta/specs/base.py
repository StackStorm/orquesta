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

import collections
import inspect
import json
import jsonschema
import logging
import re
import six
import yaml

from orquesta import exceptions as exc
from orquesta.expressions import base as expr_base
from orquesta.specs import types as spec_types
from orquesta.utils import expression as expr_util
from orquesta.utils import parameters as args_util
from orquesta.utils import schema as schema_util
from orquesta.utils import strings as str_util


LOG = logging.getLogger(__name__)


def isspec(value):
    return inspect.isclass(value) and issubclass(value, Spec)


class Spec(object):
    _catalog = None

    _version = "1.0"

    _schema = {"type": "object"}

    _meta_schema = {
        "type": "object",
        "properties": {
            "name": spec_types.NONEMPTY_STRING,
            "version": spec_types.VERSION,
            "description": spec_types.NONEMPTY_STRING,
            "tags": spec_types.UNIQUE_STRING_LIST,
        },
    }

    _schema_validator = None

    # Put the name of the spec properties in the order of validation.
    _context_evaluation_sequence = []

    # Put the name of the spec properties that are inputs for the context.
    _context_inputs = []

    def getattr_default(self, name, meta=False):
        properties = (
            self._meta_schema.get("properties", {}) if meta else self._schema.get("properties", {})
        )

        attr = properties.get(name, {})

        if inspect.isclass(attr) and issubclass(attr, Spec):
            return attr._schema.get("default", None)

        return attr.get("default", None)

    # Override __getattr__ so we can dynamically map class attributes to spec properties.
    # Per documentation, __getattr__ is called by __getattribute__ on AttributeError. In
    # this case, the attribute does not physically exist on the class and so __getattr__
    # is called which it is overridden here to access the spec dict.
    def __getattr__(self, name):
        # Retrieve from spec if attribute is a meta schema property.
        if name in self._meta_schema.get("properties", {}):
            return self.spec.get(name, self.getattr_default(name, meta=True))

        if name.replace("_", "-") in self._meta_schema.get("properties", {}):
            return self.spec.get(name.replace("_", "-"), self.getattr_default(name, meta=True))

        # Retrieve from spec if attribute is a schema property.
        if name in self._schema.get("properties", {}):
            return self.spec.get(name, self.getattr_default(name))

        if name.replace("_", "-") in self._schema.get("properties", {}):
            return self.spec.get(name.replace("_", "-"), self.getattr_default(name))

        # Retrieve from spec if attribute match a regex pattern in the schema.
        for pattern in self._schema.get("patternProperties", {}).keys():
            if re.match(pattern, name):
                return self.spec.get(name)

        # Use default for all other attributes.
        return self.__getattribute__(name)

    def __init__(self, spec, name=None, member=False):
        """jsonSchema specifications

        :param spec: json
        :param name: str
        :param member: boolean
            if True then property_specs  and
            regex_property_specs are calcuated using
            only self._schema
            if False property_specs and regex_property_specs are combined from
            self._schema and self._meta_schema
        """
        self._schema = self.get_schema(includes=None, resolve_specs=False)
        self._meta_schema = self.get_meta_schema()

        if not spec:
            raise ValueError("The spec cannot be type of None.")

        self.spec = (
            yaml.safe_load(spec)
            if not isinstance(spec, dict) and not isinstance(spec, list)
            else spec
        )

        if not isinstance(self.spec, dict) and not isinstance(spec, list):
            raise ValueError("The spec is not type of json or yaml.")

        if name:
            self.name = name

        self.member = member

        schema = (
            self._schema
            if member
            else schema_util.merge_schema(self.get_meta_schema(), self._schema)
        )

        # Process attributes defined under properties in the schema.
        property_specs = {k: v for k, v in six.iteritems(schema.get("properties", {})) if isspec(v)}

        for name, spec_cls in six.iteritems(property_specs):
            if self.spec.get(name):
                setattr(self, name, spec_cls(self.spec.get(name), member=True))

        # Process pattern properties (regex) defined in the schema.
        regex_property_specs = {
            k: v for k, v in six.iteritems(schema.get("patternProperties", {})) if isspec(v)
        }
        # regex_property_specs are member=True so they don't use meta_schema
        for pattern, spec_cls in six.iteritems(regex_property_specs):
            for name, value in six.iteritems(self.spec):
                if re.match(pattern, name) and value:
                    setattr(self, name, spec_cls(value, member=True))

    def copy(self):
        return self.deserialize(self.serialize())

    def serialize(self):
        value = {"catalog": self.get_catalog(), "version": self.get_version(), "spec": self.spec}

        if hasattr(self, "name") and self.name:
            value["name"] = self.name

        if hasattr(self, "member") and self.member:
            value["member"] = self.member

        return value

    @classmethod
    def deserialize(cls, data):
        if data.get("catalog") != cls.get_catalog():
            raise ValueError('Serialized spec catalog does not match "%s".' % cls.get_catalog())

        if data.get("version") != cls.get_version():
            raise ValueError('Serialized spec version does not match "%s".' % cls.get_version())

        return cls(data["spec"], name=data.get("name"), member=data.get("member", False))

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
            meta_schema = schema_util.merge_schema(meta_schema, parent_meta_schema)

        return schema_util.merge_schema(meta_schema, cls._meta_schema)

    @classmethod
    def get_schema(cls, includes=["meta"], resolve_specs=True):
        schema = {}

        bases = [b for b in cls.__bases__ if issubclass(b, Spec)]

        for base_cls in bases:
            parent_schema = base_cls.get_schema(includes=None)
            schema = schema_util.merge_schema(schema, parent_schema)

        schema = schema_util.merge_schema(schema, cls._schema)

        if includes and "meta" in includes:
            meta_schema = cls.get_meta_schema()
            schema = schema_util.merge_schema(schema, meta_schema)

        if not resolve_specs:
            return schema

        # Resolve the schema for children specs under properties.
        for k, v in six.iteritems(schema.get("properties", {})):
            if inspect.isclass(v) and issubclass(v, Spec):
                schema["properties"][k] = v.get_schema(includes=None)

        # Resolve the schema for children specs under patternProperties.
        for k, v in six.iteritems(schema.get("patternProperties", {})):
            if inspect.isclass(v) and issubclass(v, Spec):
                schema["patternProperties"][k] = v.get_schema(includes=None)

        # Resolve the schema for children specs under items.
        items_schema = schema.get("items", {})

        if inspect.isclass(items_schema) and issubclass(items_schema, Spec):
            schema["items"] = items_schema.get_schema(includes=None)
        elif isinstance(items_schema, dict):
            for k, v in six.iteritems(items_schema.get("properties", {})):
                if inspect.isclass(v) and issubclass(v, Spec):
                    schema_properties = schema["items"]["properties"]
                    schema_properties[k] = v.get_schema(includes=None)

        return schema

    def get_spec_path(self, prop_name, parent=None):
        return (parent.get("spec_path") + "." + prop_name).strip(".") if parent else prop_name

    def get_schema_path(self, prop_name, parent=None):
        return (
            (parent.get("schema_path") + "." + "properties." + prop_name).strip(".")
            if parent
            else "properties." + prop_name
        )

    def inspect(self, app_ctx=None, raise_exception=False):
        if app_ctx and not isinstance(app_ctx, dict):
            raise TypeError("Application context is not type of dict.")

        errors = {}
        app_ctx_metadata = None

        def sort_errors(e):
            return (e["schema_path"], e["spec_path"])

        syntax_errors = sorted(self.inspect_syntax(), key=sort_errors)

        if syntax_errors:
            errors["syntax"] = syntax_errors

        semantic_errors = sorted(self.inspect_semantics(), key=sort_errors)

        if semantic_errors:
            errors["semantics"] = semantic_errors

        expr_errors = sorted(self.inspect_expressions(), key=sort_errors)

        if expr_errors:
            errors["expressions"] = expr_errors

        if app_ctx:
            app_ctx_metadata = {"ctx": app_ctx.keys(), "spec_path": ".", "schema_path": "."}

        ctx_errors, _ = self.inspect_context(parent=app_ctx_metadata)
        ctx_errors = sorted(ctx_errors, key=sort_errors)

        if ctx_errors:
            errors["context"] = ctx_errors

        if errors and raise_exception:
            raise exc.WorkflowInspectionError(errors)

        return errors

    def inspect_syntax(self):
        result = []
        validator = self.get_schema_validator()

        for e in validator.iter_errors(self.spec):
            spec_path = ""
            schema_path = ""

            for s in list(e.absolute_path):
                spec_path += (
                    "[" + str(s) + "]" if isinstance(s, int) else "." + s if spec_path else s
                )

            for s in list(e.absolute_schema_path):
                schema_path += (
                    "[" + str(s) + "]" if isinstance(s, int) else "." + s if schema_path else s
                )

            entry = {
                "message": str_util.unescape(e.message),
                "spec_path": spec_path or None,
                "schema_path": schema_path or None,
            }

            result.append(entry)

        return result

    def inspect_semantics(self, parent=None):
        if parent and not parent.get("spec_path", None):
            raise ValueError("Parent context is missing spec path.")

        if parent and not parent.get("schema_path", None):
            raise ValueError("Parent context is missing schema path.")

        errors = []
        properties = {}
        schema = self.get_schema(includes=None)

        for prop_name, prop_type in six.iteritems(schema.get("properties", {})):
            properties[prop_name] = getattr(self, prop_name)

        for prop_name_regex, prop_type in six.iteritems(schema.get("patternProperties", {})):
            for prop_name in [key for key in self.keys() if re.findall(prop_name_regex, key)]:
                properties[prop_name] = getattr(self, prop_name)

        for prop_name, prop_value in six.iteritems(properties):
            spec_path = self.get_spec_path(prop_name, parent=parent)
            schema_path = self.get_schema_path(prop_name, parent=parent)
            prop_parent = {"spec_path": spec_path, "schema_path": schema_path}

            if isinstance(prop_value, SequenceSpec):
                errors.extend(prop_value.inspect_semantics(parent=prop_parent))

                for i in range(0, len(prop_value)):
                    item = prop_value[i]
                    item_spec_path = spec_path + "[" + str(i) + "]"
                    item_schema_path = schema_path + ".items"
                    item_parent = {"spec_path": item_spec_path, "schema_path": item_schema_path}
                    errors.extend(item.inspect_semantics(parent=item_parent))

                continue

            if isinstance(prop_value, MappingSpec):
                errors.extend(prop_value.inspect_semantics(parent=prop_parent))

                for k, v in six.iteritems(prop_value):
                    item_spec_path = spec_path + "." + k
                    item_schema_path = schema_path + ".patternProperties.^\\w+$"
                    item_parent = {"spec_path": item_spec_path, "schema_path": item_schema_path}
                    errors.extend(v.inspect_semantics(parent=item_parent))

                continue

            if isinstance(prop_value, Spec):
                errors.extend(prop_value.inspect_semantics(parent=prop_parent))
                continue

        return errors

    def inspect_expressions(self, parent=None):
        if parent and not parent.get("spec_path", None):
            raise ValueError("Parent context is missing spec path.")

        if parent and not parent.get("schema_path", None):
            raise ValueError("Parent context is missing schema path.")

        errors = []
        properties = {}
        schema = self.get_schema(includes=None)

        for prop_name, prop_type in six.iteritems(schema.get("properties", {})):
            properties[prop_name] = getattr(self, prop_name)

        for prop_name_regex, prop_type in six.iteritems(schema.get("patternProperties", {})):
            for prop_name in [key for key in self.keys() if re.findall(prop_name_regex, key)]:
                properties[prop_name] = getattr(self, prop_name)

        for prop_name, prop_value in six.iteritems(properties):
            spec_path = self.get_spec_path(prop_name, parent=parent)
            schema_path = self.get_schema_path(prop_name, parent=parent)

            if isinstance(prop_value, SequenceSpec):
                for i in range(0, len(prop_value)):
                    item = prop_value[i]
                    item_spec_path = spec_path + "[" + str(i) + "]"
                    item_schema_path = schema_path + ".items"
                    item_parent = {"spec_path": item_spec_path, "schema_path": item_schema_path}
                    errors.extend(item.inspect_expressions(parent=item_parent))

                continue

            if isinstance(prop_value, MappingSpec):
                for k, v in six.iteritems(prop_value):
                    item_spec_path = spec_path + "." + k
                    item_schema_path = schema_path + ".patternProperties.^\\w+$"
                    item_parent = {"spec_path": item_spec_path, "schema_path": item_schema_path}
                    errors.extend(v.inspect_expressions(parent=item_parent))

                continue

            if isinstance(prop_value, Spec):
                item_parent = {"spec_path": spec_path, "schema_path": schema_path}
                errors.extend(prop_value.inspect_expressions(parent=item_parent))
                continue

            result = expr_base.validate(prop_value).get("errors", [])

            for entry in result:
                entry["spec_path"] = spec_path
                entry["schema_path"] = schema_path

            errors += result

        return errors

    def inspect_context(self, parent=None):
        if parent and not parent.get("spec_path", None):
            raise ValueError("Parent context is missing spec path.")

        if parent and not parent.get("schema_path", None):
            raise ValueError("Parent context is missing schema path.")

        def decorate_ctx_var(variable, spec_path, schema_path):
            return {
                "type": variable[0],
                "expression": variable[1],
                "name": variable[2],
                "spec_path": spec_path,
                "schema_path": schema_path,
            }

        def decorate_ctx_var_error(var, msg):
            error = expr_util.format_error(
                var["type"], var["expression"], msg, var["spec_path"], var["schema_path"]
            )

            return error

        def get_ctx_inputs(prop_name, prop_value):
            ctx_inputs = []

            # By default, context inputs support only dictionary,
            # list of single item dictionaries, or string spec_types.
            if isinstance(prop_value, dict):
                ctx_inputs = list(prop_value.keys())
            elif isinstance(prop_value, list):
                for prop_value_item in prop_value:
                    if isinstance(prop_value_item, six.string_types):
                        ctx_inputs.append(prop_value_item)
                    elif isinstance(prop_value_item, dict) and len(prop_value_item) == 1:
                        ctx_inputs.extend(list(prop_value_item.keys()))
            elif isinstance(prop_value, six.string_types):
                ctx_inputs.append(prop_value)

            return ctx_inputs

        def inspect_ctx(prop_name, prop_value, spec_path, schema_path, rolling_ctx, errors):
            ctx_vars = []

            for var in expr_base.extract_vars(prop_value):
                ctx_vars.append(decorate_ctx_var(var, spec_path, schema_path))

            for ctx_var in ctx_vars:
                if ctx_var["name"].startswith("__"):
                    err_msg = (
                        'Variable "%s" that is prefixed with double underscores is considered '
                        "a private variable and cannot be referenced." % ctx_var["name"]
                    )
                    errors.append(decorate_ctx_var_error(ctx_var, err_msg))

                if ctx_var["name"] not in rolling_ctx:
                    err_msg = 'Variable "%s" is referenced before assignment.' % ctx_var["name"]
                    errors.append(decorate_ctx_var_error(ctx_var, err_msg))

            if prop_name in self._context_inputs:
                updated_ctx = get_ctx_inputs(prop_name, prop_value)
                rolling_ctx = list(set(rolling_ctx + updated_ctx))

            return rolling_ctx, errors

        errors = []
        parent_ctx = parent.get("ctx", []) if parent else []
        rolling_ctx = list(set(parent_ctx))

        for prop_name in self._context_evaluation_sequence:
            prop_value = getattr(self, prop_name)

            if not prop_value:
                continue

            spec_path = self.get_spec_path(prop_name, parent=parent)
            schema_path = self.get_schema_path(prop_name, parent=parent)

            # Pass the inspection downstream if value is a spec.
            if isinstance(prop_value, Spec):
                item_parent = {
                    "ctx": rolling_ctx,
                    "spec_path": spec_path,
                    "schema_path": schema_path,
                }

                result = prop_value.inspect_context(parent=item_parent)
                errors.extend(result[0])
                rolling_ctx = list(set(rolling_ctx + result[1]))

                continue

            # Parse inline parameters from value if value is a string.
            if isinstance(prop_value, six.string_types):
                inline_params = args_util.parse_inline_params(prop_value)

                if inline_params:
                    prop_value = inline_params

            # Preserve evaluation order if value is a list.
            if isinstance(prop_value, list):
                for i in range(0, len(prop_value)):
                    rolling_ctx, errors = inspect_ctx(
                        prop_name,
                        prop_value[i],
                        spec_path + "[" + str(i) + "]",
                        schema_path,
                        rolling_ctx,
                        errors,
                    )

                continue

            # Otherwise evaluate the value as is.
            rolling_ctx, errors = inspect_ctx(
                prop_name, prop_value, spec_path, schema_path, rolling_ctx, errors
            )

        return (sorted(errors, key=lambda x: x["spec_path"]), rolling_ctx)


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

    def __contains__(self, item):
        return item in self.spec

    def __iter__(self):
        for key in self.keys():
            yield (key, getattr(self, key))

    def __unicode__(self):
        return str_util.unicode(repr(self.spec))

    def copy(self):
        name = self.name if "name" not in self.spec and hasattr(self, "name") else None

        return self.__class__(self.spec.copy(), name=name, member=self.member)

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
            self._schema
            if member
            else schema_util.merge_schema(self.get_meta_schema(), self._schema)
        )

        if schema.get("type") != "array":
            raise exc.SchemaDefinitionError("The schema for SequenceSpec must be type of array.")

        if not isspec(schema.get("items")):
            raise exc.SchemaDefinitionError("The item type for the array must be type of Spec.")

        if not isinstance(self.spec, list):
            raise ValueError("The spec is not type of list.")

        spec_cls = schema["items"]
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
