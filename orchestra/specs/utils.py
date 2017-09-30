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

import logging
import yaml

from orchestra import specs


LOG = logging.getLogger(__name__)


def convert_wf_def_to_spec(definition):
    if not definition:
        raise ValueError('Workflow definition is empty.')

    wf_def = (
        definition
        if isinstance(definition, dict)
        else yaml.safe_load(definition)
    )

    version = str(wf_def.pop('version', specs.VERSION))

    if version != specs.VERSION:
        raise ValueError(
            'Workflow definition is not the supported version "%s".',
            specs.VERSION
        )

    if not wf_def.keys():
        raise ValueError('Workflow definition contains no workflow.')

    if len(wf_def.keys()) > 1:
        raise ValueError('Workflow definition contains more than one workflow.')

    wf_name, wf_spec = list(wf_def.items())[0]

    return specs.WorkflowSpec(wf_spec, name=wf_name)
