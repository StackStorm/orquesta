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

import abc
import six
import unittest


@six.add_metaclass(abc.ABCMeta)
class WorkflowGraphTest(unittest.TestCase):

    def _zip_wf_graph_meta(self, wf_graph_json):
        wf_graph_json['adjacency'] = [
            sorted(link, key=lambda x: x['id']) if link else link
            for link in wf_graph_json['adjacency']
        ]

        wf_graph_meta = sorted(
            zip(wf_graph_json['nodes'], wf_graph_json['adjacency']),
            key=lambda x: x[0]['id']
        )

        return wf_graph_meta

    def _assert_graph_equal(self, wf_graph, expected_wf_graph):
        wf_graph_meta = self._zip_wf_graph_meta(wf_graph.serialize())
        expected_wf_graph_meta = self._zip_wf_graph_meta(expected_wf_graph)

        self.assertListEqual(wf_graph_meta, expected_wf_graph_meta)
