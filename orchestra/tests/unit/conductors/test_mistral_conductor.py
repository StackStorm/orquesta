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
import six
from six.moves import queue

from orchestra import composition
from orchestra import symphony
from orchestra.tests.unit.composers import base


class MistralWorkflowConductorTest(base.WorkflowComposerTest):

    @classmethod
    def setUpClass(cls):
        super(MistralWorkflowConductorTest, cls).setUpClass()
        cls.composer_name = 'mistral'

    def _serialize_conductor(self, conductor):
        return {
            'scores': {
                name: score.serialize()
                for name, score in six.iteritems(conductor.scores)
            },
            'plot': conductor.plot.serialize(),
            'entry': conductor.entry
        }

    def _deserialize_conductor(self, data):
        scores = {
            name: composition.WorkflowScore.deserialize(score_json)
            for name, score_json in six.iteritems(data['scores'])
        }

        plot = composition.WorkflowExecution.deserialize(data['plot'])

        return symphony.WorkflowConductor(
            scores,
            entry=data['entry'],
            plot=plot
        )

    def _assert_conducting_sequences(self, workflow, expected_seq, **kwargs):
        sequence = []
        q = queue.Queue()

        scores = self._compose_workflow_scores(workflow)
        conductor = symphony.WorkflowConductor(scores, entry=workflow)
        context = copy.deepcopy(kwargs)

        for task_id, attributes in six.iteritems(conductor.start_workflow()):
            attributes['id'] = task_id
            q.put(attributes)

        # serialize conductor
        conductor_json = self._serialize_conductor(conductor)

        while not q.empty():
            queued_task = q.get()

            # mock completion of the task
            sequence.append(queued_task['score'] + '.' + queued_task['name'])
            completed_task = copy.deepcopy(queued_task)
            completed_task['state'] = 'succeeded'

            # deserialize conductor
            conductor = self._deserialize_conductor(conductor_json)

            next_tasks = conductor.on_task_complete(
                completed_task,
                context=context
            )

            for next_task in next_tasks:
                q.put(next_task)

            # serialize conductor
            conductor_json = self._serialize_conductor(conductor)

        self.assertListEqual(sorted(expected_seq), sorted(sequence))

    def test_sequential(self):
        workflow = 'sequential'

        expected_seq = [
            workflow + '.task1',
            workflow + '.task2',
            workflow + '.task3'
        ]

        self._assert_conducting_sequences(workflow, expected_seq)

    def test_branching(self):
        workflow = 'branching'

        expected_seq = [
            workflow + '.task1',
            workflow + '.task2',
            workflow + '.task4',
            workflow + '.task3',
            workflow + '.task5'
        ]

        self._assert_conducting_sequences(workflow, expected_seq)

    def test_join(self):
        workflow = 'join'

        expected_seq = [
            workflow + '.task1',
            workflow + '.task2',
            workflow + '.task4',
            workflow + '.task3',
            workflow + '.task5',
            workflow + '.task6',
            workflow + '.task7'
        ]

        self._assert_conducting_sequences(workflow, expected_seq)

    def test_shared_branching(self):
        workflow = 'shared_branching'

        expected_seq = [
            workflow + '.task1',
            workflow + '.task2',
            workflow + '.task3',
            workflow + '.task4',
            workflow + '.task5',
            workflow + '.task6',
            workflow + '.task7',
            workflow + '.task4',
            workflow + '.task5',
            workflow + '.task6',
            workflow + '.task7'
        ]

        self._assert_conducting_sequences(workflow, expected_seq)

    def test_decision_tree(self):
        workflow = 'decision'

        self._assert_conducting_sequences(
            workflow,
            [workflow + '.t1', workflow + '.a'],
            which='a'
        )

        self._assert_conducting_sequences(
            workflow,
            [workflow + '.t1', workflow + '.b'],
            which='b'
        )

        self._assert_conducting_sequences(
            workflow,
            [workflow + '.t1', workflow + '.c'],
            which='c'
        )
