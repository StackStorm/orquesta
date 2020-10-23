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


class WorkflowSpecValidationTest(test_base.OrchestraWorkflowSpecTest):
    def test_success(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - when: <% succeeded() %>
                    publish: foo="bar"
                    do: task2
              task2:
                action: core.noop
                next:
                  - when: <% succeeded() %>
                    publish: bar="foo"
                    do: task3
              task3:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_empty_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next: []
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_basic_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - do:
                      - task2
              task2:
                action: core.noop
                next:
                  - do: task3
              task3:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_publish_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - publish: foo="bar" bar="foo"
                    do: task2
              task2:
                action: std.echo
                input:
                    message: <% ctx().foo + ctx().bar %>
                next:
                  - publish:
                      - foobar: fubar
                      - fubar: foobar
                    do: task3
              task3:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

    def test_bad_when_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - when:
                      - foobar
                    do: task2
              task2:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "syntax": [
                {
                    "message": "['foobar'] is not of type 'string'",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.next.items.properties.when.type"
                    ),
                    "spec_path": "tasks.task1.next[0].when",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_bad_publish_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - publish:
                      - foobar
                    do: task2
              task2:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "syntax": [
                {
                    "message": "['foobar'] is not valid under any of the given schemas",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.next.items.properties.publish.oneOf"
                    ),
                    "spec_path": "tasks.task1.next[0].publish",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_bad_do_in_task_transition(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - do:
                      task2: foobar
              task2:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "syntax": [
                {
                    "message": "{'task2': 'foobar'} is not valid under any of the given schemas",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.next.items.properties.do.oneOf"
                    ),
                    "spec_path": "tasks.task1.next[0].do",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_missing_task_list(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "syntax": [
                {
                    "message": "'tasks' is a required property",
                    "schema_path": "required",
                    "spec_path": None,
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_empty_task_list(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks: {}
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "syntax": [
                {
                    "message": "{} does not have enough properties",
                    "schema_path": "properties.tasks.minProperties",
                    "spec_path": "tasks",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_fail_multiple_inspection(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            vars:
              - macro: polo
            tasks:
              task1:
                action: core.local cmd=<% ctx().foobar %>
                next:
                  - when: <% succeeded() %>
                    do:
                      - task2
              task2:
                action: core.local
                input:
                    - cmd: echo <% ctx().macro %>
                next:
                  - when: <% <% succeeded() %>
                    do: task3
              task3:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "expressions": [
                {
                    "spec_path": "tasks.task2.next[0].when",
                    "expression": "<% <% succeeded() %>",
                    "message": (
                        "Parse error: unexpected '<' at position 0 of "
                        "expression '<% succeeded()'"
                    ),
                    "type": "yaql",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.next.items.properties.when"
                    ),
                }
            ],
            "context": [
                {
                    "spec_path": "tasks.task1.input",
                    "expression": "<% ctx().foobar %>",
                    "message": 'Variable "foobar" is referenced before assignment.',
                    "type": "yaql",
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.input",
                }
            ],
            "syntax": [
                {
                    "spec_path": "tasks.task2.input",
                    "message": (
                        "[{'cmd': 'echo <% ctx().macro %>'}] is "
                        "not valid under any of the given schemas"
                    ),
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$." "properties.input.oneOf"
                    ),
                }
            ],
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_missing_task_node(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - do: task2
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "semantics": [
                {
                    "message": 'The task "task2" is not defined.',
                    "spec_path": "tasks.task1.next[0].do",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.next.items.properties.do"
                    ),
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_reserved_task_name_used(self):
        wf_def = """
            version: 1.0
            description: A basic sequential workflow.
            tasks:
              task1:
                action: core.noop
                next:
                  - do: noop
              noop:
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "semantics": [
                {
                    "message": 'The task name "noop" is reserved with special function.',
                    "spec_path": "tasks.noop",
                    "schema_path": "properties.tasks.patternProperties.^\\w+$",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_unreachable_task(self):
        wf_def = """
            version: 1.0

            description: >
              A basic workflow that demonstrate multiple splits where a post split
              task5 is also being joined with task1 before the split. The task5 is
              not going to be unreachable because the reference at task1 is out of
              scope for the reference below task4.

            tasks:
              task1:
                action: core.noop
                next:
                  - when: <% succeeded() %>
                    do: task2, task3, task5

              # branch 1
              task2:
                action: core.noop
                next:
                  - when: <% succeeded() %>
                    do: task4

              # branch 2
              task3:
                action: core.noop
                next:
                  - when: <% succeeded() %>
                    do: task4

              # split branch
              task4:
                action: core.noop
                next:
                  - when: <% succeeded() %>
                    do: task5

              task5:
                join: all
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "semantics": [
                {
                    "message": (
                        'The join task "task5" is unreachable. A join task is determined to be '
                        "unreachable if there are nested forks from multi-referenced tasks "
                        "that join on the said task. This is ambiguous to the workflow engine "
                        "because it does not know at which level should the join occurs."
                    ),
                    "spec_path": "tasks.task5",
                    "schema_path": "properties.tasks.patternProperties.^\\w+$",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_unidentified_start_tasks(self):
        wf_def = """
            version: 1.0

            description: A sample workflow with a single task loop.

            input:
              - count: 0

            tasks:
              task1:
                action: core.noop
                next:
                  - when: <% ctx().count < 2 %>
                    publish:
                      - count: <% ctx().count + 1 %>
                    do: task1
        """

        wf_spec = self.instantiate(wf_def)

        expected_errors = {
            "semantics": [
                {
                    "message": (
                        "Unable to identify any tasks to start the workflow. "
                        "If the workflow has a loop, please add a task that "
                        "serves as an entry point (or lead) into the loop."
                    ),
                    "spec_path": "tasks",
                    "schema_path": "properties.tasks",
                }
            ]
        }

        self.assertDictEqual(wf_spec.inspect(), expected_errors)
