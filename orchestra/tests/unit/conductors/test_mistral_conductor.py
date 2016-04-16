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

import copy
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

        q = queue.Queue()

        for task in conductor.start_workflow():
            q.put(task)

        while not q.empty():
            queued_task = q.get()

            # mock completion of the task
            sequence.append(queued_task['score'] + '.' + queued_task['name'])
            completed_task = copy.deepcopy(queued_task)
            completed_task['state'] = 'succeeded'

            for task in conductor.on_task_complete(completed_task):
                q.put(task)

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
            workflow + '.task4',
            workflow + '.task4',
            workflow + '.task5',
            workflow + '.task5',
            workflow + '.task6',
            workflow + '.task6'
        ]

        self._assert_conducting_sequences(workflow, expected_sequence)
