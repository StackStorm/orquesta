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

from orchestra.specs import utils
from orchestra.tests.unit import base


class DirectWorkflowSpecValidationTest(base.WorkflowSpecTest):
    fixture_rel_path = 'direct'

    def test_success(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
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

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        self.assertDictEqual(wf_spec.validate(), {})

    def test_missing_task_list(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
        """

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': '\'tasks\' is a required property',
                    'schema_path': 'required',
                    'spec_path': None
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_empty_task_list(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
                tasks: {}
        """

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': '{} does not have enough properties',
                    'schema_path': 'properties.tasks.minProperties',
                    'spec_path': 'tasks'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_task_default_list(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
                task-defaults:
                    timeout: 60
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        self.assertDictEqual(wf_spec.validate(), {})

    def test_empty_task_default_list(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
                task-defaults: {}
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        self.assertDictEqual(wf_spec.validate(), {})
