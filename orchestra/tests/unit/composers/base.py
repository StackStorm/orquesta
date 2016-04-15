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

from orchestra.utils import plugin
from orchestra.tests.fixtures import loader


@six.add_metaclass(abc.ABCMeta)
class WorkflowComposerTest(unittest.TestCase):
    composer_name = None

    def _get_fixture_path(self, wf_name):
        return self.composer_name + '/' + wf_name + '.yaml'

    def _get_workflow_definition(self, wf_name):
        return loader.get_fixture_content(
            self._get_fixture_path(wf_name),
            'workflows'
        )

    def _compose_workflow_scores(self, wf_name):
        wf_def = self._get_workflow_definition(wf_name)
        composer = plugin.get_module('orchestra.composers', self.composer_name)
        return composer.compose(wf_def, entry=wf_name)

    def _assert_workflow_composition(self, wf_name, expected_graphs):
        wf_def = self._get_workflow_definition(wf_name)
        composer = plugin.get_module('orchestra.composers', self.composer_name)
        scores = composer.compose(wf_def, entry=wf_name)

        self.assertDictEqual(expected_graphs, composer.serialize(scores))
