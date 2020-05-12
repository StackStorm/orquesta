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

# Use copy.deepcopy instead of orquesta.utils.jsonify.deepcopy because
# the schema contains instance(s) of Spec class(es) in which the
# jsonify.deepcopy method will convert the Spec class(es) to dict.
import copy

from orquesta import exceptions as exc
from orquesta.utils import dictionary as dict_util


def get_schema_type(s):
    return (s or {}).get("type")


def check_schema_mergeable(s):
    schema_type = get_schema_type(s)

    if s and schema_type not in ["array", "object"]:
        raise exc.SchemaIncompatibleError(
            "Merge operation is only supported for schema of type array "
            "and object. The given schema is type %s." % str(schema_type)
        )


def check_schemas_compatible(s1, s2):
    s1_type = get_schema_type(s1)
    s2_type = get_schema_type(s2)

    if s1_type != s2_type:
        raise exc.SchemaIncompatibleError(
            "Merge requires both types of schema to be the same. "
            "The schema types are %s and %s." % (s1_type, s2_type)
        )

    return s1_type


def merge_schema(s1, s2, overwrite=True):
    blank_schema_templates = [{"type": "object"}, {"type": "array"}]

    if s1 and s1 not in blank_schema_templates and not s2:
        return copy.deepcopy(s1)

    if s1 and s1 in blank_schema_templates and not s2:
        return {}

    if not s1 and s2 and s2 not in blank_schema_templates:
        return copy.deepcopy(s2)

    if not s1 and s2 and s2 in blank_schema_templates:
        return {}

    if not s1 and not s2:
        return {}

    if s1 in blank_schema_templates and s2 not in blank_schema_templates:
        return copy.deepcopy(s2)

    if s1 not in blank_schema_templates and s2 in blank_schema_templates:
        return copy.deepcopy(s1)

    if s1 in blank_schema_templates and s2 in blank_schema_templates:
        return {}

    check_schema_mergeable(s1)
    check_schema_mergeable(s2)
    schema_type = check_schemas_compatible(s1, s2)
    schema = SCHEMA_MERGE_FUNCTIONS[schema_type](s1, s2, overwrite=overwrite)

    return schema


def merge_object_schema(s1, s2, overwrite=True):
    schema = {"type": "object"}

    properties = dict_util.merge_dicts(
        copy.deepcopy(s1.get("properties", {})),
        copy.deepcopy(s2.get("properties", {})),
        overwrite=overwrite,
    )

    if properties:
        schema["properties"] = properties

    required = list(
        set(copy.deepcopy(s1.get("required", []))).union(set(copy.deepcopy(s2.get("required", []))))
    )

    if required:
        schema["required"] = sorted(required)

    additional = s1.get("additionalProperties", True) and s2.get("additionalProperties", True)

    if not additional:
        schema["additionalProperties"] = additional

    pattern_properties = dict_util.merge_dicts(
        copy.deepcopy(s1.get("patternProperties", {})),
        copy.deepcopy(s2.get("patternProperties", {})),
        overwrite=overwrite,
    )

    if pattern_properties:
        schema["patternProperties"] = pattern_properties

    min_properties = (
        s1.get("minProperties", 0)
        if not overwrite
        else max(s1.get("minProperties", 0), s2.get("minProperties", 0))
    )

    if min_properties > 0:
        schema["minProperties"] = min_properties

    max_properties = (
        s1.get("maxProperties", 0)
        if not overwrite
        else min(s1.get("maxProperties", 0), s2.get("maxProperties", 0))
    )

    if max_properties > 0:
        schema["maxProperties"] = max_properties

    return schema


def merge_array_schema(s1, s2, overwrite=True):
    schema = {"type": "array"}

    def process_schema_property(name, default):
        return s2.get(name, default) if overwrite else s1.get(name, default)

    items = process_schema_property("items", [])

    if items:
        schema["items"] = items

    unique_items = process_schema_property("uniqueItems", False)

    if unique_items:
        schema["uniqueItems"] = unique_items

    min_items = process_schema_property("minItems", 0)

    if min_items > 0:
        schema["minItems"] = min_items

    max_items = process_schema_property("maxItems", 0)

    if max_items > 0:
        schema["maxItems"] = max_items

    return schema


SCHEMA_MERGE_FUNCTIONS = {"object": merge_object_schema, "array": merge_array_schema}
