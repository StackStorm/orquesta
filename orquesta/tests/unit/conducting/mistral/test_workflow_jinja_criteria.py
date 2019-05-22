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


class JinjaCriteriaConductorTest(base.MistralWorkflowConductorTest):

    def test_decision_tree_with_jinja_expr(self):
        wf_name = 'decision-jinja'

        # Test branch "a"
        expected_task_seq = [
            't1',
            'a'
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            inputs={'which': 'a'}
        )

        # Test branch "b"
        expected_task_seq = [
            't1',
            'b'
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            inputs={'which': 'b'}
        )

        # Test branch "c"
        expected_task_seq = [
            't1',
            'c'
        ]

        self.assert_conducting_sequences(
            wf_name,
            expected_task_seq,
            inputs={'which': 'c'}
        )
