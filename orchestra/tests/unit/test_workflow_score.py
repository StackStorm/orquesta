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

import unittest

from orchestra import composition


EXPECTED_GRAPH = {
    'task1': {
        'task2': {},
        'task4': {},
        'task7': {}
    },
    'task2': {
        'task3': {}
    },
    'task3': {
        'task5': {}
    },
    'task4': {
        'task5': {}
    },
    'task5': {
        'task6': {}
    },
    'task6': {},
    'task7': {
        'task8': {}
    },
    'task8': {}
}


class WorkflowScoreTest(unittest.TestCase):

    def _add_tasks(self, score):
        for i in range(1, 9):
            score.add_task('task' + str(i))

    def _add_sequences(self, score):
        score.add_sequence('task1', 'task2')
        score.add_sequence('task2', 'task3')
        score.add_sequence('task1', 'task4')
        score.add_sequence('task3', 'task5')
        score.add_sequence('task4', 'task5')
        score.add_sequence('task5', 'task6')
        score.add_sequence('task1', 'task7')
        score.add_sequence('task7', 'task8')

    def test_basic_graph(self):
        score = composition.WorkflowScore()
        self._add_tasks(score)
        self._add_sequences(score)

        self.assertDictEqual(EXPECTED_GRAPH, score.show())

    def test_skip_add_tasks(self):
        score = composition.WorkflowScore()
        self._add_sequences(score)

        self.assertDictEqual(EXPECTED_GRAPH, score.show())

    def test_duplicate_add_tasks(self):
        score = composition.WorkflowScore()
        self._add_tasks(score)
        self._add_tasks(score)
        self._add_sequences(score)

        self.assertDictEqual(EXPECTED_GRAPH, score.show())

    def test_duplicate_add_sequences(self):
        score = composition.WorkflowScore()
        self._add_tasks(score)
        self._add_sequences(score)
        self._add_sequences(score)

        self.assertDictEqual(EXPECTED_GRAPH, score.show())
