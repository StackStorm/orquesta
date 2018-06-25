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

from orchestra.tests.unit.specs.mistral import base


class WorkflowSpecValidationTest(base.MistralWorkflowSpecTest):

    def test_success(self):
        # load definition from fixture: tests/fixtures/workflows/mistral/xxx.yaml
        wf_def = self.get_wf_def("sequential-success")
        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_missing_task_list(self):
        # load definition from fixture: tests/fixtures/workflows/mistral/xxx.yaml
        wf_def = self.get_wf_def("task-list-missing")
        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': '\'tasks\' is a required property',
                    'schema_path': 'required',
                    'spec_path': None
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_empty_task_list(self):
        # load definition from fixture: tests/fixtures/workflows/mistral/xxx.yaml
        wf_def = self.get_wf_def("task-list-empty")
        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': '{} does not have enough properties',
                    'schema_path': 'properties.tasks.minProperties',
                    'spec_path': 'tasks'
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_task_default_list(self):
        # load definition from fixture: tests/fixtures/workflows/mistral/xxx.yaml
        wf_def = self.get_wf_def("task-defaults-valid")
        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_empty_task_default_list(self):
        # load definition from fixture: tests/fixtures/workflows/mistral/xxx.yaml
        wf_def = self.get_wf_def("task-defaults-empty")
        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_type_direct(self):
        # load definition from fixture: tests/fixtures/workflows/mistral/xxx.yaml
        wf_def = self.get_wf_def("type-direct")
        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_type_reverse(self):
        # load definition from fixture: tests/fixtures/workflows/mistral/xxx.yaml
        wf_def = self.get_wf_def("type-reverse")
        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_type_junk_throws_error(self):
        # load definition from fixture: tests/fixtures/workflows/mistral/xxx.yaml
        wf_def = self.get_wf_def("type-invalid")
        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': "'junk' is not one of ['reverse', 'direct']",
                    'schema_path': 'properties.type.enum',
                    'spec_path': 'type'
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)
