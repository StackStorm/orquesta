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

from orquesta.tests.unit.specs.native import base as test_base


class WorkflowSpecVarsValidationTest(test_base.OrchestraWorkflowSpecTest):
    def test_bad_vars_in_input(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            input:
              - y: <% ctx().a %>
            tasks:
              task1:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().a %>",
                    "message": 'Variable "a" is referenced before assignment.',
                    "schema_path": "properties.input",
                    "spec_path": "input[0]",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_seq_vars_in_input(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            input:
              - x
              - y: foobar
              - z: <% ctx().x %> <% ctx().y %>
            tasks:
              task1:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_bad_vars_with_yaql(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            input:
              - y
            vars:
              - foo: <% ctx().a %>
              - fooa: <% ctx().x + ctx().y + ctx().z %>
            tasks:
              task1:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().a %>",
                    "message": 'Variable "a" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[0]",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().x + ctx().y + ctx().z %>",
                    "message": 'Variable "x" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[1]",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().x + ctx().y + ctx().z %>",
                    "message": 'Variable "z" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[1]",
                },
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_bad_vars_with_jinja(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            input:
              - y
            vars:
              - foo: "{{ ctx().a }}"
              - fooa: "{{ ctx().x + ctx().y + ctx().z }}"
            tasks:
              task1:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "jinja",
                    "expression": "{{ ctx().a }}",
                    "message": 'Variable "a" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[0]",
                },
                {
                    "type": "jinja",
                    "expression": "{{ ctx().x + ctx().y + ctx().z }}",
                    "message": 'Variable "x" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[1]",
                },
                {
                    "type": "jinja",
                    "expression": "{{ ctx().x + ctx().y + ctx().z }}",
                    "message": 'Variable "z" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[1]",
                },
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_bad_vars_with_mix_yaql_and_jinja(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            input:
              - y
            vars:
              - foo: "{{ ctx().a }}"
              - fooa: <% ctx().x + ctx().y + ctx().z %>
            tasks:
              task1:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "jinja",
                    "expression": "{{ ctx().a }}",
                    "message": 'Variable "a" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[0]",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().x + ctx().y + ctx().z %>",
                    "message": 'Variable "x" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[1]",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().x + ctx().y + ctx().z %>",
                    "message": 'Variable "z" is referenced before assignment.',
                    "schema_path": "properties.vars",
                    "spec_path": "vars[1]",
                },
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_seq_vars_in_vars(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            vars:
              - foobar: foobar
              - fubar: <% ctx().foobar %>
              - barfoo: <% ctx().fubar %>
              - barfu: <% ctx().barfoo %>
            tasks:
              task1:
                action: core.echo
                input:
                  message: <% ctx().fubar %>
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_bad_vars_in_seq_tasks(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            vars:
              - foobar: foobar
            tasks:
              task1:
                action: core.echo
                input:
                  message: <% ctx().foobar %>
                next:
                  - when: <% succeeded() %>
                    do: task2
              task2:
                action: core.echo
                input:
                  message: <% ctx().foo %>
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().foo %>",
                    "message": 'Variable "foo" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task2.input",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_bad_vars_in_branches(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            vars:
              - foobar: foobar
            tasks:
              task1:
                action: core.echo
                input:
                  message: <% ctx().foobar %>
                next:
                  - when: <% succeeded() %>
                    publish: foo="bar"
                    do: task2
                  - when: <% failed() %>
                    publish: bar="foo"
                    do: task3
              task2:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
              task3:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().foo + ctx().bar %>",
                    "message": 'Variable "bar" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task2.input",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().foo + ctx().bar %>",
                    "message": 'Variable "foo" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task3.input",
                },
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_bad_vars_in_parallels(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            vars:
              - foobar: foobar
            tasks:
              task1:
                action: core.echo
                input:
                  message: <% ctx().foobar %>
                next:
                  - when: <% succeeded() %>
                    publish: foo="bar"
                    do: task2
              task2:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
              task3:
                action: core.echo
                input:
                  message: <% ctx().foobar %>
                next:
                  - when: <% succeeded() %>
                    publish: bar="foo"
                    do: task4
              task4:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().foo + ctx().bar %>",
                    "message": 'Variable "bar" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task2.input",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().foo + ctx().bar %>",
                    "message": 'Variable "foo" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task4.input",
                },
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_bad_vars_in_reusable_split_tasks(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            vars:
              - foobar: foobar
            tasks:
              task1:
                action: core.echo
                input:
                  message: <% ctx().foobar %>
                next:
                  - when: <% succeeeded() %>
                    publish: foo="bar"
                    do: task2
              task2:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
                next:
                  - when: <% succeeded() %>
                    do: task5
              task3:
                action: core.echo
                input:
                  message: <% ctx().foobar %>
                next:
                  - when: <% succeeded() %>
                    publish: bar="foo"
                    do: task4
              task4:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
                next:
                  - when: <% succeeded() %>
                    do: task5
              task5:
                action: core.echo
                input:
                  message: <% ctx().foo %>
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().foo + ctx().bar %>",
                    "message": 'Variable "bar" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task2.input",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().foo + ctx().bar %>",
                    "message": 'Variable "foo" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task4.input",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().foo %>",
                    "message": 'Variable "foo" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task5.input",
                },
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_vars_in_join_task(self):
        wf_def = """
            version: 1.0
            description: A complex branching workflow.
            vars:
              - foobar: foobar
            tasks:
              task1:
                action: core.echo
                input:
                  message: <% ctx().foobar %>
                next:
                  - when: <% succeeded() %>
                    publish: foo="bar"
                    do: task2
              task2:
                action: core.echo
                input:
                  message: <% ctx().foo %>
                next:
                  - when: <% succeeded() %>
                    do: task5
              task3:
                action: core.echo
                input:
                  message: <% ctx().foobar %>
                next:
                  - when: <% succeeded() %>
                    publish: bar="foo"
                    do: task4
              task4:
                action: core.echo
                input:
                  message: <% ctx().bar %>
                next:
                  - when: <% succeeded() %>
                    do: task5
              task5:
                join: all
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), dict())

    def test_bad_vars_in_publish(self):
        wf_def = """
            version: 1.0
            description: A basic workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - publish: foo="bar" bar="foo"
                    do: task2
              task2:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
                next:
                  - publish: foobar=<% ctx().fu + ctx().bar %>
                    do: task3
              task3:
                action: core.echo
                input:
                  message: <% ctx().fu + ctx().bar %>
                next:
                  - publish: foobar=<% ctx().fu + ctx().bar %>
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().fu + ctx().bar %>",
                    "message": 'Variable "fu" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task3.input",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().fu + ctx().bar %>",
                    "message": 'Variable "fu" is referenced before assignment.',
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$.properties."
                        "next.items.properties.publish"
                    ),
                    "spec_path": "tasks.task2.next[0].publish[0]",
                },
                {
                    "type": "yaql",
                    "expression": "<% ctx().fu + ctx().bar %>",
                    "message": 'Variable "fu" is referenced before assignment.',
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$.properties."
                        "next.items.properties.publish"
                    ),
                    "spec_path": "tasks.task3.next[0].publish[0]",
                },
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_vars_in_publish_inline_params(self):
        wf_def = """
            version: 1.0
            description: A basic workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - publish: foo="bar" bar="foo"
                    do: task2, task3
              task2:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
              task3:
                action: core.echo
                input:
                  message: <% ctx().fu + ctx().bar %>
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().fu + ctx().bar %>",
                    "message": 'Variable "fu" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                    "spec_path": "tasks.task3.input",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_seq_vars_in_publish(self):
        wf_def = """
            version: 1.0
            description: A basic workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - publish: foo="bar" bar="foo" foobar="<% ctx().foo %><% ctx().bar %>"
                    do: task2
              task2:
                action: core.echo
                input:
                  message: <% ctx().foo + ctx().bar %>
                next:
                  - publish:
                      - fu: "fu"
                      - fubar: "<% ctx().fu %><% ctx().bar %>"
                    do: task3
              task3:
                action: core.echo
                input:
                  message: <% ctx().fu %><% ctx().bar %>
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_bad_vars_in_output(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            output:
              - y: <% ctx().a %>
            tasks:
              task1:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().a %>",
                    "message": 'Variable "a" is referenced before assignment.',
                    "schema_path": "properties.output",
                    "spec_path": "output[0]",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_seq_vars_in_output(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            input:
              - a
            output:
              - x: <% ctx().a %>
              - y: <% ctx().x %>
              - z: <% ctx().x %> <% ctx().y %>
            tasks:
              task1:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_vars_in_output_on_error_handling(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            input:
              - a
            output:
              - b: <% ctx().b %>
              - x: <% ctx().a %>
              - y: <% ctx().x %>
              - z: <% ctx().x %> <% ctx().y %>
            tasks:
              task1:
                action: core.noop
                next:
                  - when: <% failed() %>
                    publish: b="foobar"
                    do: fail
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})
