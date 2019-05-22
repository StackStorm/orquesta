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

from orquesta.specs.mistral import v2 as mistral_v2_specs
from orquesta.specs.mistral.v2 import tasks as task_models
from orquesta.specs.mistral.v2 import workflows as workflow_models


VERSION = mistral_v2_specs.VERSION
deserialize = workflow_models.deserialize
instantiate = workflow_models.instantiate
TaskDefaultsSpec = task_models.TaskDefaultsSpec
TaskSpec = task_models.TaskSpec
WorkflowSpec = workflow_models.WorkflowSpec

__all__ = [
    instantiate.__name__,
    deserialize.__name__,
    WorkflowSpec.__name__,
    TaskDefaultsSpec.__name__,
    TaskSpec.__name__
]
