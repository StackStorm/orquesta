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

from orquesta.specs import native as models
from orquesta.tests.unit.specs.native import base


class TaskSpecTest(base.OrchestraWorkflowSpecTest):

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
            'syntax': [
                {
                    'message': "'foobar' does not match '%s'" % models.ItemizedSpec._items_regex,
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.items.pattern'
                    ),
                    'spec_path': 'tasks.task1.with.items'
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
            'syntax': [
                {
                    'message': "'foobar' does not match '%s'" % models.ItemizedSpec._items_regex,
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.items.pattern'
                    ),
                    'spec_path': 'tasks.task1.with.items'
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
            'syntax': [
                {
                    'message': (
                        "'foo; bar in <%% zip(list(1, 2), list(a, b)) %%>' "
                        "does not match '%s'" % models.ItemizedSpec._items_regex
                    ),
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.items.pattern'
                    ),
                    'spec_path': 'tasks.task1.with.items'
                },
                {
                    'message': (
                        "'foo bar in <%% zip(list(1, 2), list(a, b)) %%>' "
                        "does not match '%s'" % models.ItemizedSpec._items_regex
                    ),
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.items.pattern'
                    ),
                    'spec_path': 'tasks.task2.with.items'
                }
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
            'syntax': [
                {
                    'message': (
                        "'foo; bar in <%% zip(list(1, 2), list(a, b)) %%>' "
                        "does not match '%s'" % models.ItemizedSpec._items_regex
                    ),
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.items.pattern'
                    ),
                    'spec_path': 'tasks.task1.with.items'
                },
                {
                    'message': (
                        "'foo bar in <%% zip(list(1, 2), list(a, b)) %%>' "
                        "does not match '%s'" % models.ItemizedSpec._items_regex
                    ),
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.items.pattern'
                    ),
                    'spec_path': 'tasks.task2.with.items'
                }
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

        expected_items = 'x in <% ctx(xs) %>'
        t1_with_attr = getattr(wf_spec.tasks['task1'], 'with')
        self.assertEqual(t1_with_attr.items, expected_items)

        expected_items = "x in {{ ctx('xs') }}"
        t2_with_attr = getattr(wf_spec.tasks['task2'], 'with')
        self.assertEqual(t2_with_attr.items, expected_items)

        expected_items = 'x in <% ctx(xs) %>'
        expected_concurrency = '<% ctx(batch_size) %>'
        t3_with_attr = getattr(wf_spec.tasks['task3'], 'with')
        self.assertEqual(t3_with_attr.items, expected_items)
        self.assertEqual(t3_with_attr.concurrency, expected_concurrency)

        expected_items = "x in {{ ctx('xs') }}"
        t4_with_attr = getattr(wf_spec.tasks['task4'], 'with')
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

        expected_items = 'x, y in <% zip(ctx(xs), ctx(ys)) %>'
        t1_with_attr = getattr(wf_spec.tasks['task1'], 'with')
        self.assertEqual(t1_with_attr.items, expected_items)

        expected_items = "x, y in {{ zip(ctx('xs'), ctx('ys')) }}"
        t2_with_attr = getattr(wf_spec.tasks['task2'], 'with')
        self.assertEqual(t2_with_attr.items, expected_items)

        expected_items = 'x, y in <% zip(ctx(xs), ctx(ys)) %>'
        t3_with_attr = getattr(wf_spec.tasks['task3'], 'with')
        self.assertEqual(t3_with_attr.items, expected_items)

        expected_items = "x, y in {{ zip(ctx('xs'), ctx('ys')) }}"
        t4_with_attr = getattr(wf_spec.tasks['task4'], 'with')
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

        expected_items = 'x, y, z in <% zip(ctx(xs), ctx(ys), ctx(zs)) %>'
        t1_with_attr = getattr(wf_spec.tasks['task1'], 'with')
        self.assertEqual(t1_with_attr.items, expected_items)

        expected_items = "x, y, z in {{ zip(ctx('xs'), ctx('ys'), ctx('zs')) }}"
        t2_with_attr = getattr(wf_spec.tasks['task2'], 'with')
        self.assertEqual(t2_with_attr.items, expected_items)

        expected_items = 'x, y, z in <% zip(ctx(xs), ctx(ys), ctx(zs)) %>'
        t3_with_attr = getattr(wf_spec.tasks['task3'], 'with')
        self.assertEqual(t3_with_attr.items, expected_items)

        expected_items = "x, y, z in {{ zip(ctx('xs'), ctx('ys'), ctx('zs')) }}"
        t4_with_attr = getattr(wf_spec.tasks['task4'], 'with')
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
                with: " x in <% ctx(xs) %> "
                action: core.noop
              task2:
                with: "  x in <% ctx(xs) %>  "
                action: core.noop
              task3:
                with: "x  in  <% ctx(xs) %>"
                action: core.noop
              task4:
                with: "x,y in <% zip(ctx(xs), ctx(ys)) %>"
                action: core.noop
              task5:
                with: "x,  y in <% zip(ctx(xs), ctx(ys)) %>"
                action: core.noop
              task6:
                with:
                  items: " x in <% ctx(xs) %> "
                action: core.noop
              task7:
                with:
                  items: "  x in <% ctx(xs) %>  "
                action: core.noop
              task8:
                with:
                  items: "x  in  <% ctx(xs) %>"
                action: core.noop
              task9:
                with:
                  items: "x,y in <% zip(ctx(xs), ctx(ys)) %>"
                action: core.noop
              task10:
                with:
                  items: "x,  y in <% zip(ctx(xs), ctx(ys)) %>"
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected_items = ' x in <% ctx(xs) %> '
        t1_with_attr = getattr(wf_spec.tasks['task1'], 'with')
        self.assertEqual(t1_with_attr.items, expected_items)

        expected_items = '  x in <% ctx(xs) %>  '
        t2_with_attr = getattr(wf_spec.tasks['task2'], 'with')
        self.assertEqual(t2_with_attr.items, expected_items)

        expected_items = 'x  in  <% ctx(xs) %>'
        t3_with_attr = getattr(wf_spec.tasks['task3'], 'with')
        self.assertEqual(t3_with_attr.items, expected_items)

        expected_items = 'x,y in <% zip(ctx(xs), ctx(ys)) %>'
        t4_with_attr = getattr(wf_spec.tasks['task4'], 'with')
        self.assertEqual(t4_with_attr.items, expected_items)

        expected_items = 'x,  y in <% zip(ctx(xs), ctx(ys)) %>'
        t5_with_attr = getattr(wf_spec.tasks['task5'], 'with')
        self.assertEqual(t5_with_attr.items, expected_items)

        expected_items = ' x in <% ctx(xs) %> '
        t6_with_attr = getattr(wf_spec.tasks['task6'], 'with')
        self.assertEqual(t6_with_attr.items, expected_items)

        expected_items = '  x in <% ctx(xs) %>  '
        t7_with_attr = getattr(wf_spec.tasks['task7'], 'with')
        self.assertEqual(t7_with_attr.items, expected_items)

        expected_items = 'x  in  <% ctx(xs) %>'
        t8_with_attr = getattr(wf_spec.tasks['task8'], 'with')
        self.assertEqual(t8_with_attr.items, expected_items)

        expected_items = 'x,y in <% zip(ctx(xs), ctx(ys)) %>'
        t9_with_attr = getattr(wf_spec.tasks['task9'], 'with')
        self.assertEqual(t9_with_attr.items, expected_items)

        expected_items = 'x,  y in <% zip(ctx(xs), ctx(ys)) %>'
        t10_with_attr = getattr(wf_spec.tasks['task10'], 'with')
        self.assertEqual(t10_with_attr.items, expected_items)

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
            'context': [
                {
                    'type': 'yaql',
                    'expression': '<% ctx(size) %>',
                    'message': 'Variable "size" is referenced before assignment.',
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.concurrency'
                    ),
                    'spec_path': 'tasks.task1.with.concurrency'
                },
                {
                    'type': 'yaql',
                    'expression': 'x in <% ctx(xs) %>',
                    'message': 'Variable "xs" is referenced before assignment.',
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.items'
                    ),
                    'spec_path': 'tasks.task1.with.items'
                }
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
            'context': [
                {
                    'type': 'yaql',
                    'expression': 'x in <% ctx(xs) %>',
                    'message': 'Variable "xs" is referenced before assignment.',
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.'
                        'properties.with.properties.items'
                    ),
                    'spec_path': 'tasks.task1.with.items'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), expected_errors)
