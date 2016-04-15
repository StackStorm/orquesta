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

from six.moves import queue

from orchestra import symphony
from orchestra.tests.unit.composers import base


class MistralWorkflowConductorTest(base.WorkflowComposerTest):

    @classmethod
    def setUpClass(cls):
        super(MistralWorkflowConductorTest, cls).setUpClass()
        cls.composer_name = 'mistral'

    def _assert_conducting_sequences(self, workflow, expected_sequence):
        sequence = []

        scores = self._compose_workflow_scores(workflow)
        conductor = symphony.WorkflowConductor(scores, entry=workflow)

        self.assertEqual(workflow, conductor.entry)

        q = queue.Queue()

        for task_name, score in conductor.conduct():
            q.put((task_name, score))

        while not q.empty():
            task_name, score = q.get()
            sequence.append(score + '.' + task_name)
            successors = conductor.conduct(
                task=task_name,
                score=score,
                task_state='succeeded')

            for next_task_name, next_score in successors:
                q.put((next_task_name, next_score))

        self.assertListEqual(sorted(expected_sequence), sorted(sequence))

    def test_sequential(self):
        workflow = 'sequential'

        expected_sequence = [
            workflow + '.task1',
            workflow + '.task2',
            workflow + '.task3'
        ]

        self._assert_conducting_sequences(workflow, expected_sequence)

    def test_branching(self):
        workflow = 'branching'

        expected_sequence = [
            workflow + '.task1',
            workflow + '.task2',
            workflow + '.task4',
            workflow + '.task3',
            workflow + '.task5'
        ]

        self._assert_conducting_sequences(workflow, expected_sequence)

    def test_join(self):
        workflow = 'join'

        expected_sequence = [
            workflow + '.task1',
            workflow + '.task2',
            workflow + '.task4',
            workflow + '.task3',
            workflow + '.task5',
            workflow + '.task6'
        ]

        self._assert_conducting_sequences(workflow, expected_sequence)

    def test_shared_branching(self):
        workflow = 'shared_branching'

        expected_sequence = [
            workflow + '.task1',
            workflow + '.task2',
            workflow + '.task3',
            workflow + '.task4.task4',
            workflow + '.task4.task5',
            workflow + '.task4.task6',
            workflow + '.task4.task4',
            workflow + '.task4.task5',
            workflow + '.task4.task6'
        ]

        self._assert_conducting_sequences(workflow, expected_sequence)
