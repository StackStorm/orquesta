# -*- coding: utf-8 -*-

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

from orquesta import conducting
from orquesta import events
from orquesta.specs import native as specs
from orquesta import states
from orquesta.tests.unit import base


class WorkflowConductorDataFlowTest(base.WorkflowConductorTest):

    def _prep_conductor(self, context=None, inputs=None, state=None):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        input:
          - a1
          - b1: <% ctx().a1 %>

        vars:
          - a2: <% ctx().b1 %>
          - b2: <% ctx().a2 %>

        output:
          - a5: <% ctx().b4 %>
          - b5: <% ctx().a5 %>

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish:
                  - a3: <% ctx().b2 %>
                  - b3: <% ctx().a3 %>
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                publish: a4=<% ctx().b3 %> b4=<% ctx().a4 %>
                do: task3
          task3:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        kwargs = {
            'context': context if context is not None else None,
            'inputs': inputs if inputs is not None else None
        }

        conductor = conducting.WorkflowConductor(spec, **kwargs)

        if state:
            conductor.request_workflow_state(state)

        return conductor

    def assert_data_flow(self, input_value):
        inputs = {'a1': input_value}
        expected_output = {'a5': inputs['a1'], 'b5': inputs['a1']}

        conductor = self._prep_conductor(inputs=inputs, state=states.RUNNING)

        for i in range(1, len(conductor.spec.tasks) + 1):
            task_name = 'task' + str(i)
            conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.RUNNING))
            conductor.update_task_flow(task_name, events.ActionExecutionEvent(states.SUCCEEDED))

        self.assertEqual(conductor.get_workflow_state(), states.SUCCEEDED)
        self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def test_data_flow_string(self):
        self.assert_data_flow('xyz')

    def test_data_flow_integer(self):
        self.assert_data_flow(123)
        self.assert_data_flow(-123)

    def test_data_flow_float(self):
        self.assert_data_flow(99.99)
        self.assert_data_flow(-99.99)

    def test_data_flow_boolean(self):
        self.assert_data_flow(True)
        self.assert_data_flow(False)

    def test_data_flow_dict(self):
        self.assert_data_flow({'x': 123, 'y': 'abc'})

    def test_data_flow_list(self):
        self.assert_data_flow([123, 'abc', True])

    def test_data_flow_unicode(self):
        self.assert_data_flow('光合作用')
