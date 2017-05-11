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


class WorkflowSpecVarsValidationTest(base.WorkflowSpecTest):
    fixture_rel_path = 'direct'

    def test_bad_vars_yaql(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
                input:
                    - y
                vars:
                    foo: <% $.a %>
                    fooa: <% $.x + $.y + $.z %>
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        expected_errors = {
            'context': [
                {
                    'type': 'yaql',
                    'expression': '<% $.a %>',
                    'message': 'Variable "a" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.foo'
                },
                {
                    'type': 'yaql',
                    'expression': '<% $.x + $.y + $.z %>',
                    'message': 'Variable "x" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.fooa'
                },
                {
                    'type': 'yaql',
                    'expression': '<% $.x + $.y + $.z %>',
                    'message': 'Variable "z" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.fooa'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_bad_vars_jinja(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
                input:
                    - y
                vars:
                    foo: "{{ _.a }}"
                    fooa: "{{ _.x + _.y + _.z }}"
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        expected_errors = {
            'context': [
                {
                    'type': 'jinja',
                    'expression': '{{ _.a }}',
                    'message': 'Variable "a" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.foo'
                },
                {
                    'type': 'jinja',
                    'expression': '{{ _.x + _.y + _.z }}',
                    'message': 'Variable "x" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.fooa'
                },
                {
                    'type': 'jinja',
                    'expression': '{{ _.x + _.y + _.z }}',
                    'message': 'Variable "z" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.fooa'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)

    def test_bad_vars_mix_yaql_and_jinja(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: A basic sequential workflow.
                type: direct
                input:
                    - y
                vars:
                    foo: "{{ _.a }}"
                    fooa: <% $.x + $.y + $.z %>
                tasks:
                    task1:
                        action: std.noop
        """

        wf_spec = utils.convert_wf_def_to_spec(wf_def)

        expected_errors = {
            'context': [
                {
                    'type': 'jinja',
                    'expression': '{{ _.a }}',
                    'message': 'Variable "a" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.foo'
                },
                {
                    'type': 'yaql',
                    'expression': '<% $.x + $.y + $.z %>',
                    'message': 'Variable "x" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.fooa'
                },
                {
                    'type': 'yaql',
                    'expression': '<% $.x + $.y + $.z %>',
                    'message': 'Variable "z" is referenced before assignment.',
                    'schema_path': 'properties.vars',
                    'spec_path': 'sequential.vars.fooa'
                }
            ]
        }

        self.assertDictEqual(wf_spec.validate(), expected_errors)
