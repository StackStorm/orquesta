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

from orquesta.specs.mistral.v2 import base
from orquesta.specs.mistral.v2.tasks import TaskDefaultsSpec
from orquesta.specs.mistral.v2.tasks import TaskSpec
from orquesta.specs.mistral.v2.workflows import deserialize
from orquesta.specs.mistral.v2.workflows import instantiate
from orquesta.specs.mistral.v2.workflows import WorkflowSpec

VERSION = base.Spec.get_version()

__all__ = [
    instantiate.__name__,
    deserialize.__name__,
    WorkflowSpec.__name__,
    TaskDefaultsSpec.__name__,
    TaskSpec.__name__
]
