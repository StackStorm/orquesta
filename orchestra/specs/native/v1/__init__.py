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

from orchestra.specs.native.v1 import base
from orchestra.specs.native.v1.models import deserialize
from orchestra.specs.native.v1.models import instantiate
from orchestra.specs.native.v1.models import TaskMappingSpec
from orchestra.specs.native.v1.models import TaskSpec
from orchestra.specs.native.v1.models import TaskTransitionSequenceSpec
from orchestra.specs.native.v1.models import TaskTransitionSpec
from orchestra.specs.native.v1.models import WorkflowSpec

VERSION = base.Spec.get_version()

__all__ = [
    instantiate.__name__,
    deserialize.__name__,
    WorkflowSpec.__name__,
    TaskMappingSpec.__name__,
    TaskSpec.__name__,
    TaskTransitionSequenceSpec.__name__,
    TaskTransitionSpec.__name__
]
