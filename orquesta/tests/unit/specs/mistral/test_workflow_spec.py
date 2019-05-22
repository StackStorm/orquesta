# Copyright 2019 Extreme Networks, Inc.
#
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

from orquesta.specs import mistral as mistral_specs
from orquesta.tests.unit.specs.mistral import base as test_base


class WorkflowSpecTest(test_base.MistralWorkflowSpecTest):

    def test_exception_empty_definition(self):
        self.assertRaises(ValueError, mistral_specs.WorkflowSpec, None, {})
        self.assertRaises(ValueError, mistral_specs.WorkflowSpec, None, '')
        self.assertRaises(ValueError, mistral_specs.WorkflowSpec, None, None)

    def test_basic_spec_serialization(self):
        wf_name = 'sequential'
        wf_spec_1 = self.get_wf_spec(wf_name)
        wf_spec_2 = mistral_specs.WorkflowSpec.deserialize(wf_spec_1.serialize())

        self.assertDictEqual(wf_spec_2.serialize(), wf_spec_1.serialize())

    def test_get_next_tasks(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertListEqual(
            wf_spec.tasks.get_next_tasks('task1'),
            [('task2', None, 'on-success'), ('task3', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_next_tasks('task2'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_next_tasks('task3'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_next_tasks('task4'),
            [('task5', None, 'on-success'), ('task6', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_next_tasks('task5'),
            [('task7', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_next_tasks('task6'),
            [('task7', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_next_tasks('task7'),
            []
        )

    def test_get_prev_tasks(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertListEqual(
            wf_spec.tasks.get_prev_tasks('task1'),
            []
        )

        self.assertListEqual(
            wf_spec.tasks.get_prev_tasks('task2'),
            [('task1', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_prev_tasks('task3'),
            [('task1', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_prev_tasks('task4'),
            [('task2', None, 'on-success'), ('task3', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_prev_tasks('task5'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_prev_tasks('task6'),
            [('task4', None, 'on-success')]
        )

        self.assertListEqual(
            wf_spec.tasks.get_prev_tasks('task7'),
            [('task5', None, 'on-success'), ('task6', None, 'on-success')]
        )

    def test_get_start_tasks(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertListEqual(
            wf_spec.tasks.get_start_tasks(),
            [('task1', None, None)]
        )

    def test_is_join_task(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertFalse(wf_spec.tasks.is_join_task('task4'))
        self.assertTrue(wf_spec.tasks.is_join_task('task7'))

    def test_is_split_task(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertTrue(wf_spec.tasks.is_split_task('task4'))
        self.assertFalse(wf_spec.tasks.is_split_task('task7'))

    def test_not_in_cycle(self):
        wf_name = 'split'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertFalse(wf_spec.tasks.in_cycle('task4'))

    def test_has_cycles(self):
        wf_name = 'cycle'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertTrue(wf_spec.tasks.has_cycles())

    def test_in_cycle(self):
        wf_name = 'cycle'
        wf_spec = self.get_wf_spec(wf_name)

        self.assertTrue(wf_spec.tasks.in_cycle('task1'))
