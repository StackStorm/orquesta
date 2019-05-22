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

from orquesta.tests.unit.composition.mistral import base


class JinjaCriteriaComposerTest(base.MistralWorkflowComposerTest):

    def test_decision_tree_with_jinja_expr(self):
        wf_name = 'decision-jinja'

        expected_wf_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 't1'
                },
                {
                    'id': 'a'
                },
                {
                    'id': 'b'
                },
                {
                    'id': 'c'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'a',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            't1',
                            condition='on-success',
                            expr="{{ ctx().which == 'a' }}"
                        )
                    },
                    {
                        'id': 'b',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            't1',
                            condition='on-success',
                            expr="{{ ctx().which == 'b' }}"
                        )
                    },
                    {
                        'id': 'c',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            't1',
                            condition='on-success',
                            expr="{{ not ctx().which in ['a', 'b'] }}"
                        )
                    }
                ],
                [],
                [],
                []
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_graph(wf_name, expected_wf_graph)

        expected_wf_ex_graph = {
            'directed': True,
            'graph': {},
            'nodes': [
                {
                    'id': 't1'
                },
                {
                    'id': 'a'
                },
                {
                    'id': 'b'
                },
                {
                    'id': 'c'
                }
            ],
            'adjacency': [
                [
                    {
                        'id': 'a',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            't1',
                            condition='on-success',
                            expr="{{ ctx().which == 'a' }}"
                        )
                    },
                    {
                        'id': 'b',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            't1',
                            condition='on-success',
                            expr="{{ ctx().which == 'b' }}"
                        )
                    },
                    {
                        'id': 'c',
                        'key': 0,
                        'criteria': self.compose_seq_expr(
                            't1',
                            condition='on-success',
                            expr="{{ not ctx().which in ['a', 'b'] }}"
                        )
                    }
                ],
                [],
                [],
                []
            ],
            'multigraph': True
        }

        self.assert_compose_to_wf_ex_graph(wf_name, expected_wf_ex_graph)
