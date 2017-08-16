# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from orchestra.specs.v2 import base
from orchestra.specs.v2.tasks import DirectTaskSpec
from orchestra.specs.v2.tasks import DirectTaskDefaultsSpec
from orchestra.specs.v2.workflows import DirectWorkflowSpec
from orchestra.specs.v2.tasks import ReverseTaskSpec
from orchestra.specs.v2.tasks import ReverseTaskDefaultsSpec
from orchestra.specs.v2.workflows import ReverseWorkflowSpec

VERSION = base.Spec.get_version()

__all__ = [
    DirectWorkflowSpec.__name__,
    DirectTaskDefaultsSpec.__name__,
    DirectTaskSpec.__name__,
    ReverseWorkflowSpec.__name__,
    ReverseTaskDefaultsSpec.__name__,
    ReverseTaskSpec.__name__
]
