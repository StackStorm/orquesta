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
from orchestra import graphing
from orchestra.specs import native as specs
from orchestra import states
from orchestra.tests.unit import base


class WorkflowConductorBasicTest(base.WorkflowConductorTest):

    def _prep_conductor(self, inputs=None, state=None):
        wf_def = """
        version: 1.0

        description: A basic sequential workflow.

        tasks:
          task1:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task2
          task2:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task3
          task3:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task4
          task4:
            action: core.noop
            next:
              - when: <% succeeded() %>
                do: task5
          task5:
            action: core.noop
        """

        spec = specs.WorkflowSpec(wf_def)

        if inputs:
            conductor = conducting.WorkflowConductor(spec, **inputs)
        else:
            conductor = conducting.WorkflowConductor(spec)

        if state:
            self.assertIsNone(conductor._workflow_state)
            self.assertIsNone(conductor.state)
            conductor.set_workflow_state(state)
            self.assertEqual(conductor._workflow_state, state)
            self.assertEqual(conductor.state, state)
        else:
            self.assertIsNone(conductor._workflow_state)
            self.assertIsNone(conductor.state)

        self.assertIsNone(conductor._graph)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)

        self.assertIsNone(conductor._flow)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)

        if inputs:
            self.assertDictEqual(conductor._inputs, inputs)
            self.assertDictEqual(conductor.inputs, inputs)
            self.assertIsNone(conductor._context, inputs)
            self.assertDictEqual(conductor.context, inputs)
        else:
            self.assertDictEqual(conductor._inputs, {})
            self.assertDictEqual(conductor.inputs, {})
            self.assertIsNone(conductor._context)
            self.assertDictEqual(conductor.context, {})

        return conductor

    def test_init(self):
        conductor = self._prep_conductor()

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'state': None,
            'flow': {
                'ready': [],
                'tasks': {},
                'sequence': []
            },
            'inputs': {},
            'context': {}
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertIsNone(conductor._workflow_state)
        self.assertIsNone(conductor.state)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)
        self.assertDictEqual(conductor._inputs, {})
        self.assertDictEqual(conductor.inputs, {})
        self.assertDictEqual(conductor._context, {})
        self.assertDictEqual(conductor.context, {})

    def test_init_with_inputs(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'state': None,
            'flow': {
                'ready': [],
                'tasks': {},
                'sequence': []
            },
            'inputs': inputs,
            'context': inputs
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertIsNone(conductor._workflow_state)
        self.assertIsNone(conductor.state)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)
        self.assertDictEqual(conductor._inputs, inputs)
        self.assertDictEqual(conductor.inputs, inputs)
        self.assertDictEqual(conductor._context, inputs)
        self.assertDictEqual(conductor.context, inputs)

    def test_serialization(self):
        inputs = {'a': 123, 'b': True}
        conductor = self._prep_conductor(inputs, states.RUNNING)

        # Mock task flows.
        for i in range(1, 6):
            task_name = 'task' + str(i)
            conductor.update_task_flow_entry(task_name, states.RUNNING)
            conductor.update_task_flow_entry(task_name, states.SUCCEEDED)

        # Serialize and check.
        data = conductor.serialize()

        expected_data = {
            'spec': conductor.spec.serialize(),
            'graph': conductor.graph.serialize(),
            'state': conductor.state,
            'flow': conductor.flow.serialize(),
            'inputs': conductor.inputs,
            'context': conductor.context
        }

        self.assertDictEqual(data, expected_data)

        # Deserialize and check.
        conductor = conducting.WorkflowConductor.deserialize(data)

        self.assertIsInstance(conductor.spec, specs.WorkflowSpec)
        self.assertIsInstance(conductor.graph, graphing.WorkflowGraph)
        self.assertEqual(len(conductor.graph._graph.node), 5)
        self.assertEqual(conductor.state, states.SUCCEEDED)
        self.assertIsInstance(conductor.flow, conducting.TaskFlow)
        self.assertEqual(len(conductor.flow.tasks), 5)
        self.assertEqual(len(conductor.flow.sequence), 5)

    def test_get_start_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        expected = [{'id': 'task1', 'name': 'task1'}]

        self.assertListEqual(conductor.get_start_tasks(), expected)

    def test_get_start_tasks_when_graph_paused(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.set_workflow_state(states.PAUSING)
        self.assertListEqual(conductor.get_start_tasks(), [])

        conductor.set_workflow_state(states.PAUSED)
        self.assertListEqual(conductor.get_start_tasks(), [])

    def test_get_start_tasks_when_graph_canceled(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.set_workflow_state(states.CANCELING)
        self.assertListEqual(conductor.get_start_tasks(), [])

        conductor.set_workflow_state(states.CANCELED)
        self.assertListEqual(conductor.get_start_tasks(), [])

    def test_get_start_tasks_when_graph_abended(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        conductor.set_workflow_state(states.FAILED)
        self.assertListEqual(conductor.get_start_tasks(), [])

    def test_get_next_tasks(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        for i in range(1, 5):
            task_name = 'task' + str(i)
            conductor.update_task_flow_entry(task_name, states.RUNNING)
            conductor.update_task_flow_entry(task_name, states.SUCCEEDED)

            next_task_name = 'task' + str(i + 1)
            expected = [{'id': next_task_name, 'name': next_task_name}]

            self.assertListEqual(conductor.get_next_tasks(task_name), expected)

    def test_get_next_tasks_when_this_task_paused(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.SUCCEEDED)
        expected = [{'id': next_task_name, 'name': next_task_name}]
        self.assertListEqual(conductor.get_next_tasks(task_name), expected)

        task_name = 'task2'
        next_task_name = 'task3'
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.PAUSING)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        conductor.update_task_flow_entry(task_name, states.PAUSED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        conductor.update_task_flow_entry(task_name, states.RESUMING)
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.SUCCEEDED)
        expected = [{'id': next_task_name, 'name': next_task_name}]
        self.assertListEqual(conductor.get_next_tasks(task_name), expected)

    def test_get_next_tasks_when_graph_paused(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.SUCCEEDED)

        expected = [{'id': next_task_name, 'name': next_task_name}]
        self.assertListEqual(conductor.get_next_tasks(task_name), expected)

        conductor.set_workflow_state(states.PAUSING)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        conductor.set_workflow_state(states.PAUSED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        conductor.set_workflow_state(states.RESUMING)
        expected = [{'id': next_task_name, 'name': next_task_name}]
        self.assertListEqual(conductor.get_next_tasks(task_name), expected)

    def test_get_next_tasks_when_this_task_canceled(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.SUCCEEDED)
        expected = [{'id': next_task_name, 'name': next_task_name}]
        self.assertListEqual(conductor.get_next_tasks(task_name), expected)

        task_name = 'task2'
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.CANCELING)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
        conductor.update_task_flow_entry(task_name, states.CANCELED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

    def test_get_next_tasks_when_graph_canceled(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.SUCCEEDED)

        expected = [{'id': next_task_name, 'name': next_task_name}]
        self.assertListEqual(conductor.get_next_tasks(task_name), expected)

        conductor.set_workflow_state(states.CANCELING)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

        conductor.set_workflow_state(states.CANCELED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

    def test_get_next_tasks_when_this_task_abended(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.SUCCEEDED)
        expected = [{'id': next_task_name, 'name': next_task_name}]
        self.assertListEqual(conductor.get_next_tasks(task_name), expected)

        task_name = 'task2'
        conductor.graph.update_transition('task2', 'task3', 0, criteria=['<% succeeded() %>'])
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.FAILED)
        self.assertEqual(conductor.state, states.FAILED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])

    def test_get_next_tasks_when_graph_abended(self):
        conductor = self._prep_conductor(state=states.RUNNING)

        task_name = 'task1'
        next_task_name = 'task2'
        conductor.update_task_flow_entry(task_name, states.RUNNING)
        conductor.update_task_flow_entry(task_name, states.SUCCEEDED)

        expected = [{'id': next_task_name, 'name': next_task_name}]
        self.assertListEqual(conductor.get_next_tasks(task_name), expected)

        conductor.set_workflow_state(states.FAILED)
        self.assertListEqual(conductor.get_next_tasks(task_name), [])
