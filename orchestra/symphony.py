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

import logging

import networkx as nx


LOG = logging.getLogger(__name__)


class WorkflowConductor(object):

    def __init__(self, scores, entry=None):
        if len(scores) < 1:
            raise Exception('No workflow score is provided.')

        if len(scores) > 1 and not entry:
            raise Exception('No entry point for multiple workflow scores.')

        if len(scores) == 1 and entry is not None and entry not in scores:
            raise Exception('Unable to find entry point in workflow scores.')

        self.scores = scores
        self.entry = entry if entry else list(scores.keys())[0]

    def conduct(self, task=None, score=None, task_state=None):
        if not score:
            score = self.entry

        if not task:
            return [(t, score) for t in self.scores[score].get_start_tasks()]

        self.scores[score].update_task(task, state=task_state)

        task_states = nx.get_node_attributes(self.scores[score]._graph, 'state')

        sequences = []

        outbounds = [
            edge
            for edge in self.scores[score]._graph.out_edges([task], data=True)
            if edge[2].get('state') == 'succeeded'
        ]

        for outbound in outbounds:
            inbounds = self.scores[score]._graph.in_edges(
                [outbound[1]],
                data=True
            )

            succeeded = [
                inbound[0]
                for inbound in inbounds
                if task_states.get(inbound[0]) == 'succeeded'
            ]

            if len(inbounds) == len(succeeded):
                sequences.append(outbound)

        def which_score(task_name, current_score):
            if '->' in task_name:
                task_name = list(task_name.split('->'))[1]
                return (task_name, current_score + '.' + task_name)
            else:
                return (task_name, current_score)

        return [which_score(sequence[1], score) for sequence in sequences]
