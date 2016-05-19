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

from orchestra import specs
from orchestra.tests.unit import base


class WorkflowSpecTest(base.WorkflowConductorTest):

    @classmethod
    def setUpClass(cls):
        cls.composer_name = 'mistral'
        super(WorkflowSpecTest, cls).setUpClass()

    def test_get_next_tasks(self):
        wf_name = 'split'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.WorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_next_tasks('task1'),
            [('task2', None, 'on-success'), ('task3', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task2'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task3'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task4'),
            [('task5', None, 'on-success'), ('task6', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task5'),
            [('task7', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task6'),
            [('task7', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_next_tasks('task7'),
            []
        )

    def test_get_prev_tasks(self):
        wf_name = 'split'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.WorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_prev_tasks('task1'),
            []
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task2'),
            [('task1', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task3'),
            [('task1', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task4'),
            [('task2', None, 'on-success'), ('task3', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task5'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task6'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.get_prev_tasks('task7'),
            [('task5', None, 'on-success'), ('task6', None, 'on-success')]
        )

    def test_get_start_tasks(self):
        wf_name = 'split'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.WorkflowSpec(wf_def)

        self.assertListEqual(
            wf_spec.get_start_tasks(),
            ['task1']
        )

    def test_is_join_task(self):
        wf_name = 'split'
        wf_def = self._get_wf_def(wf_name)
        wf_spec = specs.WorkflowSpec(wf_def)

        self.assertFalse(wf_spec.is_join_task('task4'))
        self.assertTrue(wf_spec.is_join_task('task7'))
