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

from orquesta.tests.unit.specs.mistral import base


class WorkflowSpecValidationTest(base.MistralWorkflowSpecTest):

    def test_success(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                tasks:
                    task1:
                        action: std.noop
                        on-success:
                            - task2
                    task2:
                        action: std.noop
                        on-success:
                            - task3
                    task3:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_missing_task_list(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
        """

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
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                tasks: {}
        """

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
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                task-defaults:
                    timeout: 60
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_empty_task_default_list(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                task-defaults: {}
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_workflow_type_direct(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_workflow_type_reverse(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: reverse
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_bad_workflow_type(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: junk
                tasks:
                    task1:
                        action: std.noop
        """

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
