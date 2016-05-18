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

import mock

from orchestra.composers import openstack
from orchestra.utils import plugin
from orchestra.tests.unit import base


class WorkflowComposerTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'mistral'
        super(WorkflowComposerTest, cls).setUpClass()

    def test_get_composer(self):
        self.assertEqual(
            plugin.get_module('orchestra.composers', self.composer_name),
            openstack.MistralWorkflowComposer
        )

    def test_exception_empty_definition(self):
        self.assertRaises(
            ValueError,
            self.composer.compose,
            {}
        )

    def test_exception_unidentified_entry(self):
        self.assertRaises(
            KeyError,
            self.composer.compose,
            self._get_wf_def('sequential'),
            entry='foobar'
        )

    def test_get_prev_tasks(self):
        wf_name = 'split'
        wf_def = self._get_wf_def(wf_name)
        task_specs = wf_def[wf_name]['tasks']

        self.assertListEqual(
            self.composer._get_prev_tasks(task_specs, 'task1'),
            []
        )

        self.assertListEqual(
            self.composer._get_prev_tasks(task_specs, 'task2'),
            [('task1', None, 'on-success')]
        )

        self.assertListEqual(
            self.composer._get_prev_tasks(task_specs, 'task3'),
            [('task1', None, 'on-success')]
        )

        self.assertListEqual(
            self.composer._get_prev_tasks(task_specs, 'task4'),
            [('task2', None, 'on-success'), ('task3', None, 'on-success')]
        )

        self.assertListEqual(
            self.composer._get_prev_tasks(task_specs, 'task5'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            self.composer._get_prev_tasks(task_specs, 'task6'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            self.composer._get_prev_tasks(task_specs, 'task7'),
            [('task5', None, 'on-success'), ('task6', None, 'on-success')]
        )
