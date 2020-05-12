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

from orquesta.specs import loader as spec_loader


LOG = logging.getLogger(__name__)


def instantiate(spec_type, definition):
    if not definition:
        raise ValueError("Workflow definition is empty.")

    if isinstance(definition, six.string_types):
        definition = yaml.safe_load(definition)

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

    return spec_module.instantiate(definition)


def deserialize(data):
    spec_type = data.get("catalog")
    spec_module = spec_loader.get_spec_module(spec_type)

    return spec_module.deserialize(data)
