# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specwhenic language governing permissions and
# limitations under the License.

from orchestra.tests.unit.specs.native import base


class WorkflowSpecValidationTest(base.OrchestraWorkflowSpecTest):

    def test_success(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                next:
                  - when: <% task_state(task1) = "SUCCESS" %>
                    publish: foo="bar"
                    do: task2
              task2:
                action: std.noop
                next:
                  - when: <% task_state(task2) = "SUCCESS" %>
                    publish: bar="foo"
                    do: task3
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
                next: []
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
                next:
                  - do:
                      - task2
              task2:
                action: std.noop
                next:
                  - do: task3
              task3:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.validate(), {})

    def test_publish_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                next:
                  - publish: foo="bar" bar="foo"
                    do: task2
              task2:
                action: std.echo
                input:
                    message: <% $.foo + $.bar %>
                next:
                  - publish:
                        foobar: fubar
                        fubar: foobar
                    do: task3
              task3:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.validate(), {})

    def test_bad_when_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                next:
                  - when:
                      - foobar
                    do: task2
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
                        'properties.next.items.properties.when.type'
                    ),
                    'spec_path': 'tasks.task1.next[0].when'
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
                next:
                  - publish:
                      - foobar
                    do: task2
              task2:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': "['foobar'] is not valid under any of the given schemas",
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.next.items.properties.publish.oneOf'
                    ),
                    'spec_path': 'tasks.task1.next[0].publish'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_bad_do_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: std.noop
                next:
                  - do:
                      task2: foobar
              task2:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': "{'task2': 'foobar'} is not valid under any of the given schemas",
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.next.items.properties.do.oneOf'
                    ),
                    'spec_path': 'tasks.task1.next[0].do'
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

    def test_fail_multiple_inspection(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            vars:
                macro: polo
            tasks:
              task1:
                action: core.local cmd=<% $.foobar %>
                next:
                  - when: <% succeeded() %>
                    do:
                      - task2
              task2:
                action: core.local
                input:
                    - cmd: echo <% $.macro %>
                next:
                  - when: <% <% succeeded() %>
                    do: task3
              task3:
                action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'expressions': [
                {
                    'spec_path': 'tasks.task2.next[0].when',
                    'expression': '<% <% succeeded() %>',
                    'message': (
                        'Parse error: unexpected \'<\' at position 0 of '
                        'expression \'<% succeeded()\''
                    ),
                    'type': 'yaql',
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.next.items.properties.when'
                    )
                }
            ],
            'context': [
                {
                    'spec_path': 'tasks.task1.action',
                    'expression': '<% $.foobar %>',
                    'message': 'Variable "foobar" is referenced before assignment.',
                    'type': 'yaql',
                    'schema_path': 'properties.tasks.patternProperties.^\\w+$.properties.action'
                }
            ],
            'syntax': [
                {
                    'spec_path': 'tasks.task2.input',
                    'message': '[{\'cmd\': \'echo <% $.macro %>\'}] is not of type \'object\'',
                    'schema_path': 'properties.tasks.patternProperties.^\\w+$.properties.input.type'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)
