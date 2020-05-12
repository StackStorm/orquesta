# -*- coding: utf-8 -*-

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

from orquesta import conducting
from orquesta.specs import native as native_specs
from orquesta import statuses
from orquesta.tests.unit import base as test_base
from orquesta.utils import strings as str_util
import yaql.language.utils as yaql_utils


class WorkflowConductorDataFlowTest(test_base.WorkflowConductorTest):

    wf_def_yaql = """
    version: 1.0

    description: A basic sequential workflow.

    input:
      - a1
      - b1: <% ctx().a1 %>

    vars:
      - a2: <% ctx().b1 %>
      - b2: <% ctx().a2 %>

    output:
      - a5: <% ctx().b4 %>
      - b5: <% ctx().a5 %>

    tasks:
      task1:
        action: core.noop
        next:
          - when: <% succeeded() %>
            publish:
              - a3: <% ctx().b2 %>
              - b3: <% ctx().a3 %>
            do: task2
      task2:
        action: core.noop
        next:
          - when: <% succeeded() %>
            publish: a4=<% ctx().b3 %> b4=<% ctx().a4 %>
            do: task3
      task3:
        action: core.noop
    """

    wf_def_jinja = """
    version: 1.0

    description: A basic sequential workflow.

    input:
      - a1
      - b1: '{{ ctx("a1") }}'

    vars:
      - a2: '{{ ctx("b1") }}'
      - b2: '{{ ctx("a2") }}'

    output:
      - a5: '{{ ctx("b4") }}'
      - b5: '{{ ctx("a5") }}'

    tasks:
      task1:
        action: core.noop
        next:
          - when: '{{ succeeded() }}'
            publish:
              - a3: '{{ ctx("b2") }}'
              - b3: '{{ ctx("a3") }}'
            do: task2
      task2:
        action: core.noop
        next:
          - when: '{{ succeeded() }}'
            publish: a4='{{ ctx("b3") }}' b4='{{ ctx("a4") }}'
            do: task3
      task3:
        action: core.noop
    """

    def _prep_conductor(self, wf_def, context=None, inputs=None, status=None):
        spec = native_specs.WorkflowSpec(wf_def)
        self.assertDictEqual(spec.inspect(), {})

        kwargs = {
            "context": context if context is not None else None,
            "inputs": inputs if inputs is not None else None,
        }

        conductor = conducting.WorkflowConductor(spec, **kwargs)

        if status:
            conductor.request_workflow_status(status)

        return conductor

    def _get_combined_value(self, callstack_depth=0):
        # This returns dict typed value all Python built-in type values
        # which orquesta spec could accept.
        if callstack_depth < 2:
            return {
                "null": None,
                "integer_positive": 123,
                "integer_negative": -123,
                "number_positive": 99.99,
                "number_negative": -99.99,
                "string": "xyz",
                "boolean_true": True,
                "boolean_false": False,
                "array": list(self._get_combined_value(callstack_depth + 1).values()),
                "object": self._get_combined_value(callstack_depth + 1),
            }
        else:
            return {}

    def _assert_data_flow(self, inputs, expected_output):
        # This assert method checks input value would be handled and published
        # as an expected type and value with both YAQL and Jinja expressions.
        for wf_def in [self.wf_def_jinja, self.wf_def_yaql]:
            conductor = self._prep_conductor(wf_def, inputs=inputs, status=statuses.RUNNING)

            for i in range(1, len(conductor.spec.tasks) + 1):
                task_name = "task" + str(i)
                forward_statuses = [statuses.RUNNING, statuses.SUCCEEDED]
                self.forward_task_statuses(conductor, task_name, forward_statuses)

            # Render workflow output and checkout workflow status and output.
            conductor.render_workflow_output()
            self.assertEqual(conductor.get_workflow_status(), statuses.SUCCEEDED)
            self.assertDictEqual(conductor.get_workflow_output(), expected_output)

    def assert_data_flow(self, input_value):
        inputs = {"a1": input_value}
        expected_output = {"a5": inputs["a1"], "b5": inputs["a1"]}

        self._assert_data_flow(inputs, expected_output)

    def assert_unicode_data_flow(self, input_value):
        inputs = {
            u"a1": (
                str_util.unicode(input_value, encoding_type="utf-8", force=True)
                if six.PY2
                else input_value
            )
        }

        expected_output = {u"a5": inputs["a1"], u"b5": inputs["a1"]}

        self._assert_data_flow(inputs, expected_output)

    def test_data_flow_string(self):
        self.assert_data_flow("xyz")

    def test_data_flow_integer(self):
        self.assert_data_flow(123)
        self.assert_data_flow(-123)

    def test_data_flow_float(self):
        self.assert_data_flow(99.99)
        self.assert_data_flow(-99.99)

    def test_data_flow_boolean(self):
        self.assert_data_flow(True)
        self.assert_data_flow(False)

    def test_data_flow_none(self):
        self.assert_data_flow(None)

    def test_data_flow_dict(self):
        mapping_typed_data = self._get_combined_value()

        self.assertIsInstance(mapping_typed_data, yaql_utils.MappingType)
        self.assert_data_flow(mapping_typed_data)

    def test_data_flow_list(self):
        sequence_typed_data = list(self._get_combined_value().values())

        self.assertIsInstance(sequence_typed_data, yaql_utils.SequenceType)
        self.assert_data_flow(sequence_typed_data)

    def test_data_flow_unicode(self):
        self.assert_unicode_data_flow("光合作用")
