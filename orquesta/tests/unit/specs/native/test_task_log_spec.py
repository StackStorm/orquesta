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


class LogSpecTest(base.OrchestraWorkflowSpecTest):

    def test_get_schema(self):
        expected_schema = models.LogSpec.get_schema(includes=[])
        schema = models.WorkflowSpec.get_schema()
        tasks_spec_schema = schema['properties']['tasks']
        task_spec_schema = tasks_spec_schema['patternProperties']['^\\w+$']
        task_transition_spec_schema = task_spec_schema['properties']['next']
        log_schema = task_transition_spec_schema['items']['properties']['log']

        self.assertDictEqual(log_schema, expected_schema)

    def test_inline_log_definition(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log: type="info" message="This is baloney." data=null
        """

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), dict())

    def test_bad_inline_log_definition(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log: foobar
        """

        expected_errors = {
            'syntax': [
                {
                    'message': "'message' is a required property",
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.properties.next.'
                        'items.properties.log.required'
                    ),
                    'spec_path': 'tasks.task1.next[0].log'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_log_bad_type(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log:
                      type: foobar
                      message: "This is baloney."
                      data: null
        """

        expected_errors = {
            'syntax': [
                {
                    'message': "'foobar' is not one of ['info', 'warn', 'error']",
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.properties.next.'
                        'items.properties.log.properties.type.enum'
                    ),
                    'spec_path': 'tasks.task1.next[0].log.type'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_default_log_type(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log:
                      message: "This is baloney."
                      data: null
        """

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), dict())

    def test_inline_log_bad_type(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log: type="foobar" message="This is baloney."
        """

        expected_errors = {
            'syntax': [
                {
                    'message': "'foobar' is not one of ['info', 'warn', 'error']",
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.properties.next.'
                        'items.properties.log.properties.type.enum'
                    ),
                    'spec_path': 'tasks.task1.next[0].log.type'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_message_bad_type(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log:
                      message: 1234
        """

        expected_errors = {
            'syntax': [
                {
                    'message': "1234 is not of type 'string'",
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.properties.next.'
                        'items.properties.log.properties.message.type'
                    ),
                    'spec_path': 'tasks.task1.next[0].log.message'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_message_bad_vars(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log:
                      message: <% ctx(foobar) %>
        """

        expected_errors = {
            'context': [
                {
                    'type': 'yaql',
                    'expression': '<% ctx(foobar) %>',
                    'message': 'Variable "foobar" is referenced before assignment.',
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.properties.next.'
                        'items.properties.log.properties.message'
                    ),
                    'spec_path': 'tasks.task1.next[0].log.message'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_inline_message_bad_vars(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log: message=<% ctx(foobar) %>
        """

        expected_errors = {
            'context': [
                {
                    'type': 'yaql',
                    'expression': '<% ctx(foobar) %>',
                    'message': 'Variable "foobar" is referenced before assignment.',
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.properties.next.'
                        'items.properties.log.properties.message'
                    ),
                    'spec_path': 'tasks.task1.next[0].log.message'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_data_bad_vars(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log:
                      message: "This is baloney."
                      data: <% ctx(foobar) %>
        """

        expected_errors = {
            'context': [
                {
                    'type': 'yaql',
                    'expression': '<% ctx(foobar) %>',
                    'message': 'Variable "foobar" is referenced before assignment.',
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.properties.next.'
                        'items.properties.log.properties.data'
                    ),
                    'spec_path': 'tasks.task1.next[0].log.data'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_inline_data_bad_vars(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - log: message="This is baloney." data=<% ctx(foobar) %>
        """

        expected_errors = {
            'context': [
                {
                    'type': 'yaql',
                    'expression': '<% ctx(foobar) %>',
                    'message': 'Variable "foobar" is referenced before assignment.',
                    'schema_path': (
                        'properties.tasks.patternProperties.^\\w+$.properties.next.'
                        'items.properties.log.properties.data'
                    ),
                    'spec_path': 'tasks.task1.next[0].log.data'
                }
            ]
        }

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), expected_errors)

    def test_publish_vars_reference(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - publish:
                      - foobar: baloney
                      - baloney: foobar
                    log:
                      message: <% ctx(baloney) %>
                      data: <% ctx(foobar) %>
        """

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), dict())

    def test_inline_publish_vars_reference(self):
        wf_def = """
            version: 1.0

            tasks:
              task1:
                action: core.noop
                next:
                  - publish:
                      - foobar: baloney
                      - baloney: foobar
                    log: message=<% ctx(baloney) %> data=<% ctx(foobar) %>
        """

        wf_spec = self.instantiate(wf_def)
        self.assertDictEqual(wf_spec.inspect(), dict())
