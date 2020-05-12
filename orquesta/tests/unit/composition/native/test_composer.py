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

from orquesta.composers import native as native_comp
from orquesta.tests.unit.composition.native import base as native_comp_test_base
from orquesta.utils import plugin as plugin_util


class WorkflowComposerTest(native_comp_test_base.OrchestraWorkflowComposerTest):
    def test_get_composer(self):
        self.assertEqual(
            plugin_util.get_module("orquesta.composers", self.spec_module_name),
            native_comp.WorkflowComposer,
        )
