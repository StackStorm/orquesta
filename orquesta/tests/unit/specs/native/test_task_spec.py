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
                with: foobar
                action: core.noop
        """

        expected_errors = {
            'syntax': [
                {
                    'message': (
                        "'foobar' does not match '%s'" %
                        models.TaskSpec._schema['properties']['with']['pattern']
                    ),
                    'schema_path': (
                        'properties.tasks.patternProperties.'
                        '^\\w+$.properties.with.pattern'
                    ),
                    'spec_path': 'tasks.task1.with'
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
                with: foo; bar in <% zip(list(1, 2), list(a, b)) %>
                action: core.noop
        """

        expected_errors = {
            'syntax': [
                {
                    'message': (
                        "'foo; bar in <%% zip(list(1, 2), list(a, b)) %%>' does not match '%s'" %
                        models.TaskSpec._schema['properties']['with']['pattern']
                    ),
                    'schema_path': (
                        'properties.tasks.patternProperties.'
                        '^\\w+$.properties.with.pattern'
                    ),
                    'spec_path': 'tasks.task1.with'
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

            tasks:
              task1:
                with: x in <% ctx(xs) %>
                action: core.noop
              task2:
                with: "x in {{ ctx('xs') }}"
                action: core.noop
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected = 'x in <% ctx(xs) %>'
        self.assertEqual(getattr(wf_spec.tasks['task1'], 'with'), expected)

        expected = "x in {{ ctx('xs') }}"
        self.assertEqual(getattr(wf_spec.tasks['task2'], 'with'), expected)

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
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected = 'x, y in <% zip(ctx(xs), ctx(ys)) %>'
        self.assertEqual(getattr(wf_spec.tasks['task1'], 'with'), expected)

        expected = "x, y in {{ zip(ctx('xs'), ctx('ys')) }}"
        self.assertEqual(getattr(wf_spec.tasks['task2'], 'with'), expected)

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
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected = 'x, y, z in <% zip(ctx(xs), ctx(ys), ctx(zs)) %>'
        self.assertEqual(getattr(wf_spec.tasks['task1'], 'with'), expected)

        expected = "x, y, z in {{ zip(ctx('xs'), ctx('ys'), ctx('zs')) }}"
        self.assertEqual(getattr(wf_spec.tasks['task2'], 'with'), expected)

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
        """

        wf_spec = self.instantiate(wf_def)

        self.assertDictEqual(wf_spec.inspect(), {})

        expected = ' x in <% ctx(xs) %> '
        self.assertEqual(getattr(wf_spec.tasks['task1'], 'with'), expected)

        expected = '  x in <% ctx(xs) %>  '
        self.assertEqual(getattr(wf_spec.tasks['task2'], 'with'), expected)

        expected = 'x  in  <% ctx(xs) %>'
        self.assertEqual(getattr(wf_spec.tasks['task3'], 'with'), expected)

        expected = 'x,y in <% zip(ctx(xs), ctx(ys)) %>'
        self.assertEqual(getattr(wf_spec.tasks['task4'], 'with'), expected)

        expected = 'x,  y in <% zip(ctx(xs), ctx(ys)) %>'
        self.assertEqual(getattr(wf_spec.tasks['task5'], 'with'), expected)
