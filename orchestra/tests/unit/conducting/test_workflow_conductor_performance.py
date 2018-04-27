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

import random
import string

from orchestra import conducting
from orchestra.specs import native as specs
from orchestra import states
from orchestra.tests.unit import base


class WorkflowConductorStressTest(base.WorkflowConductorTest):

    def _prep_wf_def(self, num_tasks):
        wf_def = {
            'input': ['data'],
            'tasks': {}
        }

        for i in range(1, num_tasks):
            task_name = 't' + str(i)
            next_task_name = 't' + str(i + 1)
            wf_def['tasks'][task_name] = {'action': 'core.noop', 'next': [{'do': next_task_name}]}

        task_name = 't' + str(num_tasks)
        wf_def['tasks'][task_name] = {'action': 'core.noop'}

        return wf_def

    def _prep_conductor(self, num_tasks, inputs=None, state=None):
        wf_def = self._prep_wf_def(num_tasks)
        spec = specs.WorkflowSpec(wf_def)

        if inputs:
            conductor = conducting.WorkflowConductor(spec, **inputs)
        else:
            conductor = conducting.WorkflowConductor(spec)

        if state:
            conductor.set_workflow_state(state)

        return conductor

    def test_runtime_function_of_graph_size(self):
        num_tasks = 100

        conductor = self._prep_conductor(num_tasks, state=states.RUNNING)

        for i in range(1, num_tasks + 1):
            task_name = 't' + str(i)
            conductor.update_task_flow(task_name, states.RUNNING)
            conductor.update_task_flow(task_name, states.SUCCEEDED)

        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)

    def test_serialization_function_of_graph_size(self):
        num_tasks = 100
        conductor = self._prep_conductor(num_tasks, state=states.RUNNING)
        conductor.deserialize(conductor.serialize())

    def test_serialization_function_of_data_size(self):
        data_length = 1000000
        data = ''.join(random.choice(string.ascii_lowercase) for _ in range(data_length))
        self.assertEqual(len(data), data_length)
        conductor = self._prep_conductor(1, inputs={'data': data}, state=states.RUNNING)
        conductor.deserialize(conductor.serialize())
