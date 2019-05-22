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

from orquesta import statuses
from orquesta.tests.unit.conducting.native import base


class SplitWorkflowConductorTest(base.OrchestraWorkflowConductorTest):

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

        expected_term_tasks = [
            ('task7', 1),
            ('task7', 2)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks
        )

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

        expected_term_tasks = [
            ('task8', 1),
            ('task8', 4),
            ('task8', 5)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks
        )

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

        expected_term_tasks = [
            ('task10', 3),
            ('task10', 4),
            ('task10', 5),
            ('task10', 6)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks
        )

    def test_splits_mixed(self):
        wf_name = 'splits-mixed'

        expected_routes = [
            [],
            ['task1__t0'],                  # task1 -> task3
            ['task2__t0'],                  # task2 -> task3
            ['task1__t0', 'task3__t0'],     # task1 -> task3 -> task4
            ['task2__t0', 'task3__t0']      # task2 -> task3 -> task4
        ]

        expected_task_seq = [
            ('task1', 0),
            ('task2', 0),
            ('task3', 1),
            ('task3', 2),
            ('task4', 3),
            ('task4', 4),
            ('task5', 3),
            ('task5', 4)
        ]

        expected_term_tasks = [
            ('task5', 3),
            ('task5', 4)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks
        )

    def test_splits_mixed_alt_branch(self):
        wf_name = 'splits-mixed'

        expected_routes = [
            [],
            ['task1__t0'],                  # task1 -> task3
            ['task2__t0'],                  # task2 -> task3
            ['task1__t0', 'task3__t0'],     # task1 -> task3 -> task4
            ['task2__t0', 'task3__t1']      # task2 -> task3 -> task4
        ]

        expected_task_seq = [
            ('task1', 0),
            ('task2', 0),
            ('task3', 1),
            ('task3', 2),
            ('task4', 3),
            ('task4', 4),
            ('task5', 3),
            ('task5', 4)
        ]

        mock_statuses = [
            statuses.SUCCEEDED,   # task1, 0
            statuses.SUCCEEDED,   # task2, 0
            statuses.SUCCEEDED,   # task3, 1
            statuses.FAILED,      # task3, 2
            statuses.SUCCEEDED,   # task4, 3
            statuses.SUCCEEDED,   # task4, 4
            statuses.SUCCEEDED,   # task5, 3
            statuses.SUCCEEDED    # task5, 4
        ]

        expected_term_tasks = [
            ('task5', 3),
            ('task5', 4)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            mock_statuses=mock_statuses,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks
        )

    def test_splits_multiple_transition(self):
        wf_name = 'splits-transition'

        expected_routes = [
            [],
            ['task1__t0'],                  # task1 -> task3
            ['task2__t0'],                  # task2 -> task3
            ['task1__t0', 'task3__t0'],     # task1 -> task3 -> task4 (when #1)
            ['task1__t0', 'task3__t1'],     # task1 -> task3 -> task4 (when #2)
            ['task2__t0', 'task3__t0'],     # task2 -> task3 -> task4 (when #1)
            ['task2__t0', 'task3__t1']      # task2 -> task3 -> task4 (when #2)
        ]

        expected_task_seq = [
            ('task1', 0),
            ('task2', 0),
            ('task3', 1),
            ('task3', 2),
            ('task4', 3),
            ('task4', 4),
            ('task4', 5),
            ('task4', 6),
            ('task5', 3),
            ('task5', 4),
            ('task5', 5),
            ('task5', 6),
            ('task6', 3),
            ('task6', 4),
            ('task6', 5),
            ('task6', 6),
            ('task7', 3),
            ('task7', 4),
            ('task7', 5),
            ('task7', 6)
        ]

        expected_term_tasks = [
            ('task7', 3),
            ('task7', 4),
            ('task7', 5),
            ('task7', 6)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks
        )

    def test_very_many_splits(self):
        wf_name = 'splits-very-many'

        expected_routes = [
            [],
            ['init__t1'],
            ['init__t0'],
            ['init__t0', 'task2__t0'],
            ['init__t0', 'task2__t0', 'task7__t0'],
            ['init__t0', 'task2__t0', 'task7__t0', 'task10__t0'],
            ['init__t0', 'task2__t0', 'task7__t0', 'task10__t0', 'task13__t0'],
            ['init__t0', 'task2__t0', 'task7__t0', 'task10__t0', 'task13__t0', 'task15__t0'],
            ['init__t0', 'task2__t0', 'task7__t0', 'task10__t0',
             'task13__t0', 'task15__t0', 'task17__t0']
        ]

        expected_task_seq = [
            ('init', 0),
            ('notify', 1),
            ('task2', 2),
            ('task5', 3),
            ('task7', 3),
            ('task9', 4),
            ('task10', 4),
            ('task11', 5),
            ('task12', 5),
            ('task13', 5),
            ('task14', 6),
            ('task15', 6),
            ('task17', 7),
            ('notify', 8)
        ]

        expected_term_tasks = [
            ('notify', 1),
            ('notify', 8)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks
        )

    def test_very_many_splits_alt(self):
        wf_name = 'splits-very-many'

        inputs = {
            'fork1': True,
            'fork2': True,
            'fork3': True,
            'fork4': True,
            'fork5': True,
            'fork6': True,
            'fork7': True,
            'fork8': True
        }

        mock_results = [
            None,   # init
            None,   # notify
            None,   # task1
            None,   # task2
            None,   # task3
            None,   # task4
            None,   # task5
            None,   # task6
            None,   # task8
            None,   # task9
            None,   # task11
            None,   # task12
            None,   # task13
            None,   # task14
            False,  # task16 (try #1)
            True,   # task16 (try #2)
            None,   # task17
            False,  # task18 (try #1)
            True,   # task18 (try #2)
            None,   # task19
            None    # notify
        ]

        expected_routes = [
            [],
            ['init__t0'],
            ['task1__t0'],
            ['task1__t0', 'task4__t0'],
            ['task1__t0', 'task4__t0', 'task6__t0'],
            ['task1__t0', 'task4__t0', 'task6__t0', 'task8__t0'],
            ['task1__t0', 'task4__t0', 'task6__t0', 'task8__t0', 'task9__t0'],
            ['task1__t0', 'task4__t0', 'task6__t0', 'task8__t0', 'task9__t0', 'task13__t0'],
            ['task1__t0', 'task4__t0', 'task6__t0', 'task8__t0',
             'task9__t0', 'task13__t0', 'task16__t0'],
            ['task1__t0', 'task4__t0', 'task6__t0', 'task8__t0',
             'task9__t0', 'task13__t0', 'task16__t0', 'task19__t0']
        ]

        expected_task_seq = [
            ('init', 0),
            ('notify', 1),
            ('task1', 0),
            ('task2', 2),
            ('task3', 2),
            ('task4', 2),
            ('task5', 3),
            ('task6', 3),
            ('task8', 4),
            ('task9', 5),
            ('task11', 6),
            ('task12', 6),
            ('task13', 6),
            ('task14', 7),
            ('task16', 7),
            ('task16', 7),
            ('task17', 8),
            ('task18', 8),
            ('task18', 8),
            ('task19', 8),
            ('notify', 9)
        ]

        expected_term_tasks = [
            ('notify', 1),
            ('notify', 9)
        ]

        self.assert_spec_inspection(wf_name)

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            inputs=inputs,
            mock_results=mock_results,
            expected_routes=expected_routes,
            expected_term_tasks=expected_term_tasks
        )
