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

from orquesta.specs import base as spec_base


LOG = logging.getLogger(__name__)


class Spec(spec_base.Spec):
    _catalog = "native"

    _version = "1.0"

    _meta_schema = {
        "type": "object",
        "properties": {"version": {"enum": [_version, float(_version)]}},
    }


class MappingSpec(spec_base.MappingSpec):
    _catalog = "native"

    _version = "1.0"

    _meta_schema = {
        "type": "object",
        "properties": {"version": {"enum": [_version, float(_version)]}},
    }


class SequenceSpec(spec_base.SequenceSpec):
    _catalog = "native"

    _version = "1.0"

    _meta_schema = {
        "type": "object",
        "properties": {"version": {"enum": [_version, float(_version)]}},
    }
