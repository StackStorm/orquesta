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

from orquesta.tests.unit.specs.mistral import base as test_base


class WorkflowSpecValidationTest(test_base.MistralWorkflowSpecTest):

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

    def test_on_success_conditional(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                vars:
                  xxx: true
                tasks:
                    task1:
                        action: std.noop
                        on-success:
                            - task2: "{{ _.xxx }}"
                    task2:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_on_error_conditional(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                vars:
                  xxx: true
                tasks:
                    task1:
                        action: std.noop
                        on-error:
                            - task2: "{{ _.xxx }}"
                    task2:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_on_complete_conditional(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                vars:
                  xxx: true
                tasks:
                    task1:
                        action: std.noop
                        on-complete:
                            - task2: "{{ _.xxx }}"
                    task2:
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
                   concurrency: 1
                   keep-result: true
                   retry:
                       delay: 5
                       count: 10
                   safe-rerun: true
                   wait-before: 3
                   wait-after: 9
                   pause-before: false
                   target: some_node
                   timeout: 90
                   on-success:
                       - task2
                   on-error:
                       - task2
                   on-complete:
                       - task3
                tasks:
                    task1:
                        action: std.noop
                    task2:
                        action: std.noop
                    task3:
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

    def test_workflow_with_output_on_error(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                vars:
                    jinja_expr: "abc"
                    test_jinja: "xyz"
                output-on-error:
                    stdout: "abc"
                    jinja_expr: "{{ _.test_jinja }}"
                    yaql_expr: <% $.test_yaql %>

                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_empty_output_on_error(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                output-on-error: {}
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': '{} does not have enough properties',
                    'schema_path': 'properties.output-on-error.minProperties',
                    'spec_path': 'output-on-error'
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_task_policies(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.

                tasks:
                    task1:
                        action: std.noop
                        concurrency: 1
                        keep-result: true
                        retry:
                            delay: 5
                            count: 10
                        safe-rerun: true
                        wait-before: 3
                        wait-after: 9
                        pause-before: false
                        target: some_node
                        timeout: 90
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_task_policies_with_expressions(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.

                tasks:
                    task1:
                        action: std.noop
                        concurrency: "{{ 3 | int }}"
                        keep-result: "{{ True }}"
                        retry:
                            delay: "{{ 5 | int }}"
                            count: "{{ 10 | int }}"
                        safe-rerun: "{{ True }}"
                        wait-before: "{{ 3 | int }}"
                        wait-after: "{{ 9 | int }}"
                        pause-before: "{{ False }}"
                        target: "{{ 'some_node' }}"
                        timeout: "{{ 39 | int }}"
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_task_policies_validation_errors(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.

                tasks:
                    task1:
                        action: std.noop
                        concurrency: []
                        keep-result: {}
                        retry: ''
                        safe-rerun: []
                        wait-before: []
                        wait-after: []
                        pause-before: []
                        target: {}
                        timeout: []
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            'syntax': [
                {
                    'message': '[] is not valid under any of the given schemas',
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.concurrency.oneOf'),
                    'spec_path': 'tasks.task1.concurrency'
                },
                {
                    'message': '{} is not valid under any of the given schemas',
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.keep-result.oneOf'),
                    'spec_path': 'tasks.task1.keep-result'
                },
                {
                    'message': '[] is not valid under any of the given schemas',
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.pause-before.oneOf'),
                    'spec_path': 'tasks.task1.pause-before'
                },
                {
                    'message': "'' is not of type 'object'",
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.retry.type'),
                    'spec_path': 'tasks.task1.retry'
                },
                {
                    'message': '[] is not valid under any of the given schemas',
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.safe-rerun.oneOf'),
                    'spec_path': 'tasks.task1.safe-rerun'
                },
                {
                    'message': "{} is not of type 'string'",
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.target.type'),
                    'spec_path': 'tasks.task1.target'
                },
                {
                    'message': '[] is not valid under any of the given schemas',
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.timeout.oneOf'),
                    'spec_path': 'tasks.task1.timeout'
                },
                {
                    'message': '[] is not valid under any of the given schemas',
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.wait-after.oneOf'),
                    'spec_path': 'tasks.task1.wait-after'
                },
                {
                    'message': '[] is not valid under any of the given schemas',
                    'schema_path': ('properties.tasks.patternProperties'
                                    '.^\\w+$.properties.wait-before.oneOf'),
                    'spec_path': 'tasks.task1.wait-before'
                },
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_task_publish_on_error(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.

                tasks:
                    task1:
                        action: std.noop
                        publish-on-error:
                            stdout: "{{ 'abc' }}"
                            stderr:  <$ 'xyz' %>
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})
