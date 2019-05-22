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

from orquesta.tests.unit import base as test_base


class MistralWorkflowComposerTest(test_base.WorkflowComposerTest):

    @classmethod
    def setUpClass(cls):
        cls.spec_module_name = 'mistral'
        super(MistralWorkflowComposerTest, cls).setUpClass()

    def compose_seq_expr(self, name, *args, **kwargs):
        return self.composer._compose_transition_criteria(name, *args, **kwargs)
