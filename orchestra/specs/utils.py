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


WF_SPEC_MAP = {
    'direct': specs.DirectWorkflowSpec,
    'reverse': specs.ReverseWorkflowSpec
}


def convert_wf_def_to_spec(definition):
    if not definition:
        raise ValueError('Workflow definition is empty.')

    wf_def = (
        definition
        if isinstance(definition, dict)
        else yaml.safe_load(definition)
    )

    version = str(wf_def['version'])

    if version != specs.VERSION:
        raise ValueError(
            'Workflow definition is not supported version "%s".',
            specs.VERSION
        )

    wf_names = [key for key in wf_def.keys() if key != 'version']

    if not wf_names:
        raise ValueError('Workflow definition contains no workflow.')

    wf_type = wf_def[wf_names[0]].get('type', 'direct')

    return WF_SPEC_MAP[wf_type](wf_def)
