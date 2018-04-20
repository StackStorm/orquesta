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

from orchestra import conducting
from orchestra.specs import native as specs
from orchestra import states
from orchestra.tests.unit import base


class WorkflowConductorTaskRenderingTest(base.WorkflowConductorTest):

    def _prep_conductor(self, inputs=None, state=None):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        input:
          - action_name
          - action_input

        tasks:
          task1:
            action: <% $.action_name %>
            input: <% $.action_input %>
            next:
              - do: task2
          task2:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)

        conductor = conducting.WorkflowConductor(spec)

        if inputs:
            conductor = conducting.WorkflowConductor(spec, **inputs)
        else:
            conductor = conducting.WorkflowConductor(spec)

        if state:
            conductor.set_workflow_state(state)

        return conductor

    def test_runtime_rendering(self):
        action_name = 'core.echo'
        action_input = {'message': 'All your base are belong to us!'}
        inputs = {'action_name': action_name, 'action_input': action_input}
        conductor = self._prep_conductor(inputs, state=states.RUNNING)

        task1_spec = conductor.spec.tasks.get_task('task1').copy()
        task1_spec.action = action_name
        task1_spec.input = action_input

        expected = [self.format_task_item('task1', inputs, task1_spec)]
        actual = conductor.get_start_tasks()
        self.assert_task_list(actual, expected)
