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

import six

from orquesta.specs import native as native_specs
from orquesta.tests.unit.specs.native import base as test_base


class TaskSpecTest(test_base.OrchestraWorkflowSpecTest):
    def test_delay_bad_syntax(self):
        wf_def = """
            version: 1.0
            description: A basic workflow with a delay in task.
            tasks:
              task1:
                delay: true
                action: core.noop
        """

        expected_errors = {
            "syntax": [
                {
                    "message": "True is not valid under any of the given schemas",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$." "properties.delay.oneOf"
                    ),
                    "spec_path": "tasks.task1.delay",
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_delay(self):
        wf_def = """
            version: 1.0
            description: A basic workflow with a delay in task.
            tasks:
              task1:
                delay: 100
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        task1 = wf_spec.tasks["task1"]

        self.assertEqual(task1.delay, 100)

    def test_delay_with_expression(self):
        wf_def = """
            version: 1.0
            description: A basic workflow with a delay in task.
            vars:
              - delay: 100
            tasks:
              task1:
                delay: <% ctx().delay %>
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        task1 = wf_spec.tasks["task1"]

        self.assertEqual(task1.delay, "<% ctx().delay %>")

    def test_delay_with_bad_vars(self):
        wf_def = """
            version: 1.0
            description: A basic workflow with a delay in task.
            tasks:
              task1:
                delay: <% ctx().delay %>
                action: core.noop
        """

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx().delay %>",
                    "message": 'Variable "delay" is referenced before assignment.',
                    "schema_path": "properties.tasks.patternProperties.^\\w+$.properties.delay",
                    "spec_path": "tasks.task1.delay",
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_with_items_bad_syntax(self):
        wf_def = """
            version: 1.0
            description: A basic with items workflow.
            tasks:
              task1:
                with:
                  items: foobar
                action: core.noop
        """

        expected_errors = {
            "syntax": [
                {
                    "message": (
                        "'foobar' does not match '%s'" % native_specs.ItemizedSpec._items_regex
                    ),
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.items.pattern"
                    ),
                    "spec_path": "tasks.task1.with.items",
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_inline_with_items_bad_syntax(self):
        wf_def = """
            version: 1.0
            description: A basic with items workflow.
            tasks:
              task1:
                with: foobar
                action: core.noop
        """

        expected_errors = {
            "syntax": [
                {
                    "message": (
                        "'foobar' does not match '%s'" % native_specs.ItemizedSpec._items_regex
                    ),
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.items.pattern"
                    ),
                    "spec_path": "tasks.task1.with.items",
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_multiple_with_items_bad_syntax(self):
        wf_def = """
            version: 1.0
            description: A basic with items workflow.
            tasks:
              task1:
                with:
                  items: foo; bar in <% zip(list(1, 2), list(a, b)) %>
                action: core.noop
              task2:
                with:
                  items: foo bar in <% zip(list(1, 2), list(a, b)) %>
                action: core.noop
        """

        expected_errors = {
            "syntax": [
                {
                    "message": (
                        "'foo; bar in <%% zip(list(1, 2), list(a, b)) %%>' "
                        "does not match '%s'" % native_specs.ItemizedSpec._items_regex
                    ),
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.items.pattern"
                    ),
                    "spec_path": "tasks.task1.with.items",
                },
                {
                    "message": (
                        "'foo bar in <%% zip(list(1, 2), list(a, b)) %%>' "
                        "does not match '%s'" % native_specs.ItemizedSpec._items_regex
                    ),
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.items.pattern"
                    ),
                    "spec_path": "tasks.task2.with.items",
                },
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_multiple_inline_with_items_bad_syntax(self):
        wf_def = """
            version: 1.0
            description: A basic with items workflow.
            tasks:
              task1:
                with: foo; bar in <% zip(list(1, 2), list(a, b)) %>
                action: core.noop
              task2:
                with: foo bar in <% zip(list(1, 2), list(a, b)) %>
                action: core.noop
        """

        expected_errors = {
            "syntax": [
                {
                    "message": (
                        "'foo; bar in <%% zip(list(1, 2), list(a, b)) %%>' "
                        "does not match '%s'" % native_specs.ItemizedSpec._items_regex
                    ),
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.items.pattern"
                    ),
                    "spec_path": "tasks.task1.with.items",
                },
                {
                    "message": (
                        "'foo bar in <%% zip(list(1, 2), list(a, b)) %%>' "
                        "does not match '%s'" % native_specs.ItemizedSpec._items_regex
                    ),
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.items.pattern"
                    ),
                    "spec_path": "tasks.task2.with.items",
                },
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_with_one_list(self):
        wf_def = """
            version: 1.0

            description: Send direct y to xs

            input:
              - xs
              - batch_size: 1

            tasks:
              task1:
                with: <% ctx(xs) %>
                action: core.noop
              task2:
                with: "{{ ctx('xs') }}"
                action: core.noop
              task3:
                with:
                  items: <% ctx(xs) %>
                  concurrency: <% ctx(batch_size) %>
                action: core.noop
              task4:
                with:
                  items: "{{ ctx('xs') }}"
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected_items = "<% ctx(xs) %>"
        t1_with_attr = getattr(wf_spec.tasks["task1"], "with")
        self.assertEqual(t1_with_attr.items, expected_items)

        expected_items = "{{ ctx('xs') }}"
        t2_with_attr = getattr(wf_spec.tasks["task2"], "with")
        self.assertEqual(t2_with_attr.items, expected_items)

        expected_items = "<% ctx(xs) %>"
        expected_concurrency = "<% ctx(batch_size) %>"
        t3_with_attr = getattr(wf_spec.tasks["task3"], "with")
        self.assertEqual(t3_with_attr.items, expected_items)
        self.assertEqual(t3_with_attr.concurrency, expected_concurrency)

        expected_items = "{{ ctx('xs') }}"
        t4_with_attr = getattr(wf_spec.tasks["task4"], "with")
        self.assertEqual(t4_with_attr.items, expected_items)

    def test_with_one_list_and_var_expansion(self):
        wf_def = """
            version: 1.0

            description: Send direct y to xs

            input:
              - xs
              - batch_size: 1

            tasks:
              task1:
                with: x in <% ctx(xs) %>
                action: core.noop
              task2:
                with: "x in {{ ctx('xs') }}"
                action: core.noop
              task3:
                with:
                  items: x in <% ctx(xs) %>
                  concurrency: <% ctx(batch_size) %>
                action: core.noop
              task4:
                with:
                  items: "x in {{ ctx('xs') }}"
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected_items = "x in <% ctx(xs) %>"
        t1_with_attr = getattr(wf_spec.tasks["task1"], "with")
        self.assertEqual(t1_with_attr.items, expected_items)

        expected_items = "x in {{ ctx('xs') }}"
        t2_with_attr = getattr(wf_spec.tasks["task2"], "with")
        self.assertEqual(t2_with_attr.items, expected_items)

        expected_items = "x in <% ctx(xs) %>"
        expected_concurrency = "<% ctx(batch_size) %>"
        t3_with_attr = getattr(wf_spec.tasks["task3"], "with")
        self.assertEqual(t3_with_attr.items, expected_items)
        self.assertEqual(t3_with_attr.concurrency, expected_concurrency)

        expected_items = "x in {{ ctx('xs') }}"
        t4_with_attr = getattr(wf_spec.tasks["task4"], "with")
        self.assertEqual(t4_with_attr.items, expected_items)

    def test_with_two_lists(self):
        wf_def = """
            version: 1.0

            description: Send direct customized y to xs

            input:
              - xs
              - ys

            tasks:
              task1:
                with: x, y in <% zip(ctx(xs), ctx(ys)) %>
                action: core.noop
              task2:
                with: "x, y in {{ zip(ctx('xs'), ctx('ys')) }}"
                action: core.noop
              task3:
                with:
                  items: x, y in <% zip(ctx(xs), ctx(ys)) %>
                action: core.noop
              task4:
                with:
                  items: "x, y in {{ zip(ctx('xs'), ctx('ys')) }}"
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected_items = "x, y in <% zip(ctx(xs), ctx(ys)) %>"
        t1_with_attr = getattr(wf_spec.tasks["task1"], "with")
        self.assertEqual(t1_with_attr.items, expected_items)

        expected_items = "x, y in {{ zip(ctx('xs'), ctx('ys')) }}"
        t2_with_attr = getattr(wf_spec.tasks["task2"], "with")
        self.assertEqual(t2_with_attr.items, expected_items)

        expected_items = "x, y in <% zip(ctx(xs), ctx(ys)) %>"
        t3_with_attr = getattr(wf_spec.tasks["task3"], "with")
        self.assertEqual(t3_with_attr.items, expected_items)

        expected_items = "x, y in {{ zip(ctx('xs'), ctx('ys')) }}"
        t4_with_attr = getattr(wf_spec.tasks["task4"], "with")
        self.assertEqual(t4_with_attr.items, expected_items)

    def test_with_many_lists(self):
        wf_def = """
            version: 1.0

            description: Send direct customized y to xs

            input:
              - xs
              - ys
              - zs

            tasks:
              task1:
                with: x, y, z in <% zip(ctx(xs), ctx(ys), ctx(zs)) %>
                action: core.noop
              task2:
                with: "x, y, z in {{ zip(ctx('xs'), ctx('ys'), ctx('zs')) }}"
                action: core.noop
              task3:
                with:
                  items: x, y, z in <% zip(ctx(xs), ctx(ys), ctx(zs)) %>
                action: core.noop
              task4:
                with:
                  items: "x, y, z in {{ zip(ctx('xs'), ctx('ys'), ctx('zs')) }}"
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected_items = "x, y, z in <% zip(ctx(xs), ctx(ys), ctx(zs)) %>"
        t1_with_attr = getattr(wf_spec.tasks["task1"], "with")
        self.assertEqual(t1_with_attr.items, expected_items)

        expected_items = "x, y, z in {{ zip(ctx('xs'), ctx('ys'), ctx('zs')) }}"
        t2_with_attr = getattr(wf_spec.tasks["task2"], "with")
        self.assertEqual(t2_with_attr.items, expected_items)

        expected_items = "x, y, z in <% zip(ctx(xs), ctx(ys), ctx(zs)) %>"
        t3_with_attr = getattr(wf_spec.tasks["task3"], "with")
        self.assertEqual(t3_with_attr.items, expected_items)

        expected_items = "x, y, z in {{ zip(ctx('xs'), ctx('ys'), ctx('zs')) }}"
        t4_with_attr = getattr(wf_spec.tasks["task4"], "with")
        self.assertEqual(t4_with_attr.items, expected_items)

    def test_with_various_spacing(self):
        wf_def = """
            version: 1.0

            description: Send direct y to xs

            input:
              - xs
              - ys

            tasks:
              task1:
                with: " <% ctx(xs) %> "
                action: core.noop
              task2:
                with: "  <% ctx(xs) %>  "
                action: core.noop
              task3:
                with: " x in <% ctx(xs) %> "
                action: core.noop
              task4:
                with: "  x in <% ctx(xs) %>  "
                action: core.noop
              task5:
                with: "x  in  <% ctx(xs) %>"
                action: core.noop
              task6:
                with: "x,y in <% zip(ctx(xs), ctx(ys)) %>"
                action: core.noop
              task7:
                with: "x,  y in <% zip(ctx(xs), ctx(ys)) %>"
                action: core.noop
              task8:
                with:
                  items: " x in <% ctx(xs) %> "
                action: core.noop
              task9:
                with:
                  items: "  x in <% ctx(xs) %>  "
                action: core.noop
              task10:
                with:
                  items: "x  in  <% ctx(xs) %>"
                action: core.noop
              task11:
                with:
                  items: "x,y in <% zip(ctx(xs), ctx(ys)) %>"
                action: core.noop
              task12:
                with:
                  items: "x,  y in <% zip(ctx(xs), ctx(ys)) %>"
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected_items = {
            "task1": " <% ctx(xs) %> ",
            "task2": "  <% ctx(xs) %>  ",
            "task3": " x in <% ctx(xs) %> ",
            "task4": "  x in <% ctx(xs) %>  ",
            "task5": "x  in  <% ctx(xs) %>",
            "task6": "x,y in <% zip(ctx(xs), ctx(ys)) %>",
            "task7": "x,  y in <% zip(ctx(xs), ctx(ys)) %>",
            "task8": " x in <% ctx(xs) %> ",
            "task9": "  x in <% ctx(xs) %>  ",
            "task10": "x  in  <% ctx(xs) %>",
            "task11": "x,y in <% zip(ctx(xs), ctx(ys)) %>",
            "task12": "x,  y in <% zip(ctx(xs), ctx(ys)) %>",
        }

        for task_name, items_expr in six.iteritems(expected_items):
            tk_with_attr = getattr(wf_spec.tasks[task_name], "with")
            self.assertEqual(tk_with_attr.items, items_expr)

    def test_with_items_bad_vars(self):
        wf_def = """
            version: 1.0
            description: A basic with items workflow.
            tasks:
              task1:
                with:
                  items: x in <% ctx(xs) %>
                  concurrency: <% ctx(size) %>
                action: core.noop
        """

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "<% ctx(size) %>",
                    "message": 'Variable "size" is referenced before assignment.',
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.concurrency"
                    ),
                    "spec_path": "tasks.task1.with.concurrency",
                },
                {
                    "type": "yaql",
                    "expression": "x in <% ctx(xs) %>",
                    "message": 'Variable "xs" is referenced before assignment.',
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.items"
                    ),
                    "spec_path": "tasks.task1.with.items",
                },
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_inline_with_items_bad_vars(self):
        wf_def = """
            version: 1.0
            description: A basic with items workflow.
            tasks:
              task1:
                with: x in <% ctx(xs) %>
                action: core.noop
        """

        expected_errors = {
            "context": [
                {
                    "type": "yaql",
                    "expression": "x in <% ctx(xs) %>",
                    "message": 'Variable "xs" is referenced before assignment.',
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.with.properties.items"
                    ),
                    "spec_path": "tasks.task1.with.items",
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_retry_basic(self):
        wf_def = """
            version: 1.0
            tasks:
              task1:
                action: core.noop
                retry:
                  when: <% failed() %>
                  count: 3
                  delay: 10
        """

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), {})

    def test_retry_missing_when_count_delay(self):
        wf_def = """
            version: 1.0
            tasks:
              task1:
                action: core.noop
                retry: {}
        """

        expected_errors = {
            "syntax": [
                {
                    "message": "'count' is a required property",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$." "properties.retry.required"
                    ),
                    "spec_path": "tasks.task1.retry",
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_retry_bad_vars(self):
        wf_def = """
            version: 1.0
            tasks:
              task1:
                action: core.noop
                retry:
                  # should be a string
                  when: 1
                  # should be an int
                  count:
                    - "abc"
                  # should be a number
                  delay: true
        """

        expected_errors = {
            "syntax": [
                {
                    "message": "['abc'] is not valid under any of the given schemas",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.retry.properties.count.oneOf"
                    ),
                    "spec_path": "tasks.task1.retry.count",
                },
                {
                    "message": "True is not valid under any of the given schemas",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.retry.properties.delay.oneOf"
                    ),
                    "spec_path": "tasks.task1.retry.delay",
                },
                {
                    "message": "1 is not of type 'string'",
                    "schema_path": (
                        "properties.tasks.patternProperties.^\\w+$."
                        "properties.retry.properties.when.type"
                    ),
                    "spec_path": "tasks.task1.retry.when",
                },
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)
