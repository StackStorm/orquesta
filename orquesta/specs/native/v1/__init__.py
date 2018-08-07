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

from orquesta.specs.native.v1 import base
from orquesta.specs.native.v1.models import deserialize                 # noqa
from orquesta.specs.native.v1.models import instantiate                 # noqa
from orquesta.specs.native.v1.models import TaskMappingSpec             # noqa
from orquesta.specs.native.v1.models import TaskSpec                    # noqa
from orquesta.specs.native.v1.models import TaskTransitionSequenceSpec  # noqa
from orquesta.specs.native.v1.models import TaskTransitionSpec          # noqa
from orquesta.specs.native.v1.models import WorkflowSpec                # noqa

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
