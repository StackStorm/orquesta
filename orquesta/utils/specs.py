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
import six
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from yaml import constructor
from yaml import nodes

from orquesta import exceptions as exc
from orquesta.specs import loader as spec_loader


LOG = logging.getLogger(__name__)


# Custom YAML loader that throw an exception on duplicate key.
# Credit: https://gist.github.com/pypt/94d747fe5180851196eb
class UniqueKeyLoader(Loader):
    def construct_mapping(self, node, deep=False):
        if not isinstance(node, nodes.MappingNode):
            raise constructor.ConstructorError(
                None, None, "expected a mapping node, but found %s" % node.id, node.start_mark
            )
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError as exc:
                raise constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found unacceptable key (%s)" % exc,
                    key_node.start_mark,
                )
            # check for duplicate keys
            if key in mapping:
                raise constructor.ConstructorError('found duplicate key "%s"' % key_node.value)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


# Add UniqueKeyLoader to the yaml SafeLoader so it is invoked by safe_load.
yaml.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    UniqueKeyLoader.construct_mapping,
    Loader=yaml.SafeLoader,
)


def instantiate(spec_type, definition):
    if not definition:
        raise ValueError("Workflow definition is empty.")

    if isinstance(definition, six.string_types):
        try:
            definition = yaml.safe_load(definition)
        except constructor.ConstructorError as e:
            raise exc.WorkflowInspectionError([e])

    if not isinstance(definition, dict):
        raise ValueError("Unable to convert workflow definition into dict.")

    spec_module = spec_loader.get_spec_module(spec_type)

    version = definition.pop("version", None)

    if not version:
        raise ValueError("Version of the workflow definition is not provided.")

    spec_version = spec_module.VERSION

    if str(version) != str(spec_version):
        raise ValueError('Workflow definition is not the supported version "%s".', spec_version)

    if not definition.keys():
        raise ValueError("Workflow definition contains no workflow.")

    spec = spec_module.instantiate(definition)

    return spec


def deserialize(data):
    spec_type = data.get("catalog")
    spec_module = spec_loader.get_spec_module(spec_type)

    return spec_module.deserialize(data)
