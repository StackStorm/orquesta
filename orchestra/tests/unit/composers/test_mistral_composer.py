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

from orchestra.composers import openstack
from orchestra.utils import plugin
from orchestra.tests.unit.composers import base


class MistralWorkflowComposerTest(base.WorkflowComposerTest):

    @classmethod
    def setUpClass(cls):
        super(MistralWorkflowComposerTest, cls).setUpClass()
        cls.composer_name = 'mistral'

    @classmethod
    def _get_seq_criteria(cls, name, state):
        composer = plugin.get_module('orchestra.composers', cls.composer_name)

        return {'criteria': composer.compose_sequence_criteria(name, state)}

    def test_get_composer(self):
        self.assertEqual(
            plugin.get_module('orchestra.composers', self.composer_name),
            openstack.MistralWorkflowComposer
        )

    def test_exception_empty_definition(self):
        composer = plugin.get_module('orchestra.composers', self.composer_name)

        self.assertRaises(
            ValueError,
            composer.compose,
            {}
        )

    def test_exception_unidentified_entry(self):
        composer = plugin.get_module('orchestra.composers', self.composer_name)

        self.assertRaises(
            KeyError,
            composer.compose,
            self._get_workflow_definition('sequential'),
            entry='foobar'
        )

    def test_sequential(self):
        workflow = 'sequential'

        expected_graphs = {
            workflow: {
                'task1': {
                    'task2': {0: self._get_seq_criteria('task1', 'succeeded')}
                },
                'task2': {
                    'task3': {0: self._get_seq_criteria('task2', 'succeeded')}
                },
                'task3': {}
            }
        }

        self._assert_workflow_composition(workflow, expected_graphs)

    def test_branching(self):
        workflow = 'branching'

        expected_graphs = {
            workflow: {
                'task1': {
                    'task2': {0: self._get_seq_criteria('task1', 'succeeded')},
                    'task4': {0: self._get_seq_criteria('task1', 'succeeded')}
                },
                'task2': {
                    'task3': {0: self._get_seq_criteria('task2', 'succeeded')}
                },
                'task3': {},
                'task4': {
                    'task5': {0: self._get_seq_criteria('task4', 'succeeded')}
                },
                'task5': {}
            }
        }

        self._assert_workflow_composition(workflow, expected_graphs)

    def test_join(self):
        workflow = 'join'

        expected_graphs = {
            workflow: {
                'task1': {
                    'task2': {0: self._get_seq_criteria('task1', 'succeeded')},
                    'task4': {0: self._get_seq_criteria('task1', 'succeeded')}
                },
                'task2': {
                    'task3': {0: self._get_seq_criteria('task2', 'succeeded')}
                },
                'task3': {
                    'task6': {0: self._get_seq_criteria('task3', 'succeeded')}
                },
                'task4': {
                    'task5': {0: self._get_seq_criteria('task4', 'succeeded')}
                },
                'task5': {
                    'task6': {0: self._get_seq_criteria('task5', 'succeeded')}
                },
                'task6': {}
            }
        }

        self._assert_workflow_composition(workflow, expected_graphs)

    def test_shared_branching(self):
        workflow = 'shared_branching'

        expected_graphs = {
            workflow: {
                'task1': {
                    'task2': {0: self._get_seq_criteria('task1', 'succeeded')},
                    'task3': {0: self._get_seq_criteria('task1', 'succeeded')}
                },
                'task2': {
                    'task4': {0: self._get_seq_criteria('task2', 'succeeded')}
                },
                'task3': {
                    'task4': {0: self._get_seq_criteria('task3', 'succeeded')}
                },
                'task4': {
                    'task5': {0: self._get_seq_criteria('task4', 'succeeded')}
                },
                'task5': {
                    'task6': {0: self._get_seq_criteria('task5', 'succeeded')}
                },
                'task6': {}
            }
        }

        self._assert_workflow_composition(workflow, expected_graphs)
