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

from orchestra.tests.unit.specs.default import base


class WorkflowSpecValidationTest(base.OrchestraWorkflowSpecTest):

    def test_success(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                on-complete:
                  - if: <% task_state(task1) = "SUCCESS" %>
                    publish:
                      foo: bar
                    next: task2
              task2:
                action: std.noop
                on-complete:
                  - if: <% task_state(task2) = "SUCCESS" %>
                    publish:
                      bar: foo
                    next: task3
              task3:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.validate(), {})

    def test_empty_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                on-complete: []
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.validate(), {})

    def test_basic_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                on-complete:
                  - next:
                      - task2
              task2:
                action: std.noop
                on-complete:
                  - next: task3
              task3:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.validate(), {})

    def test_bad_if_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                on-complete:
                  - if:
                      - foobar
                    next: task2
              task2:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': "['foobar'] is not of type 'string'",
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.on-complete.items.properties.if.type'
                    ),
                    'spec_path': 'tasks.task1.on-complete[0].if'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_bad_publish_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                on-complete:
                  - publish:
                      - foobar
                    next: task2
              task2:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': "['foobar'] is not of type 'object'",
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.on-complete.items.properties.publish.type'
                    ),
                    'spec_path': 'tasks.task1.on-complete[0].publish'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_bad_next_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                on-complete:
                  - next:
                      task2: foobar
              task2:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': (
                        "{'task2': 'foobar'} is not valid "
                        "under any of the given schemas"
                    ),
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.on-complete.items.properties.next.oneOf'
                    ),
                    'spec_path': 'tasks.task1.on-complete[0].next'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_missing_task_list(self):
        wf_def = """
            version: 1.0
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

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_empty_task_list(self):
        wf_def = """
            version: 1.0
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

        self.assertDictEqual(wf_spec.validate(), expected_errors)
