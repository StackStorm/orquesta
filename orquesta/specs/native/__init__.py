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

from orquesta.specs.native import v1 as native_v1_specs
from orquesta.specs.native.v1 import models as native_v1_models


VERSION = native_v1_specs.VERSION
instantiate = native_v1_models.instantiate
deserialize = native_v1_models.deserialize
WorkflowSpec = native_v1_models.WorkflowSpec
ItemizedSpec = native_v1_models.ItemizedSpec
TaskMappingSpec = native_v1_models.TaskMappingSpec
TaskSpec = native_v1_models.TaskSpec
TaskTransitionSequenceSpec = native_v1_models.TaskTransitionSequenceSpec
TaskTransitionSpec = native_v1_models.TaskTransitionSpec

__all__ = [
    instantiate.__name__,
    deserialize.__name__,
    WorkflowSpec.__name__,
    ItemizedSpec.__name__,
    TaskMappingSpec.__name__,
    TaskSpec.__name__,
    TaskTransitionSequenceSpec.__name__,
    TaskTransitionSpec.__name__,
]
