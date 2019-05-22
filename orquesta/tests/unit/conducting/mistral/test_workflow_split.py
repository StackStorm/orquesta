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

from orquesta.tests.unit.conducting.mistral import base


class SplitWorkflowConductorTest(base.MistralWorkflowConductorTest):

    def test_split(self):
        wf_name = 'split'

        expected_routes = [
            [],                             # default from start
            ['task2__t0'],                  # task1 -> task2 -> task4
            ['task3__t0']                   # task1 -> task3 -> task4
        ]

        expected_task_seq = [
            ('task1', 0),
            ('task2', 0),
            ('task3', 0),
            ('task4', 1),
            ('task4', 2),
            ('task5', 1),
            ('task5', 2),
            ('task6', 1),
            ('task6', 2),
            ('task7', 1),
            ('task7', 2)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq, expected_routes)

    def test_splits(self):
        wf_name = 'splits'

        expected_routes = [
            [],                             # default from start
            ['task1__t0'],                  # task1 -> task8
            ['task2__t0'],                  # task1 -> task2 -> task4
            ['task3__t0'],                  # task1 -> task3 -> task4
            ['task2__t0', 'task7__t0'],     # task1 -> task2 -> task4 -> task5/6 -> task7 -> task8
            ['task3__t0', 'task7__t0']      # task1 -> task3 -> task4 -> task5/6 -> task7 -> task8
        ]

        expected_task_seq = [
            ('task1', 0),
            ('task2', 0),
            ('task3', 0),
            ('task8', 1),
            ('task4', 2),
            ('task4', 3),
            ('task5', 2),
            ('task5', 3),
            ('task6', 2),
            ('task6', 3),
            ('task7', 2),
            ('task7', 3),
            ('task8', 4),
            ('task8', 5)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq, expected_routes)

    def test_nested_splits(self):
        wf_name = 'splits-nested'

        expected_routes = [
            [],                             # default from start
            ['task2__t0'],                  # task1 -> task2 -> task4
            ['task3__t0'],                  # task1 -> task3 -> task4
            ['task2__t0', 'task5__t0'],     # task1 -> task2 -> task4 -> task5 -> task7
            ['task3__t0', 'task5__t0'],     # task1 -> task3 -> task4 -> task5 -> task7
            ['task2__t0', 'task6__t0'],     # task1 -> task2 -> task4 -> task6 -> task7
            ['task3__t0', 'task6__t0']      # task1 -> task3 -> task4 -> task6 -> task7
        ]

        expected_task_seq = [
            ('task1', 0),
            ('task2', 0),
            ('task3', 0),
            ('task4', 1),
            ('task4', 2),
            ('task5', 1),
            ('task5', 2),
            ('task6', 1),
            ('task6', 2),
            ('task7', 3),
            ('task7', 4),
            ('task7', 5),
            ('task7', 6),
            ('task8', 3),
            ('task8', 4),
            ('task8', 5),
            ('task8', 6),
            ('task9', 3),
            ('task9', 4),
            ('task9', 5),
            ('task9', 6),
            ('task10', 3),
            ('task10', 4),
            ('task10', 5),
            ('task10', 6)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(wf_name, expected_task_seq, expected_routes)
