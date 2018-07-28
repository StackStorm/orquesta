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

from orquesta.tests.unit import base as common_base
from orquesta.tests.unit.composition.mistral import base as mistral_base


class MistralWorkflowConductorTest(
    mistral_base.MistralWorkflowComposerTest,
    common_base.WorkflowConductorTest
):
    pass
