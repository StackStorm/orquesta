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


try:
    from unittest import mock
except ImportError:
    import mock

import sys
import unittest

from orquesta.specs.mock.models import TestCaseSpec
import orquesta.tests.exceptions as exc
from orquesta.tests.mocks import WorkflowTestFixture
from orquesta.tests.unit.conducting.native import base


class FixtureTest(base.OrchestraWorkflowConductorTest):
    def test_with_items_failed(self):
        wf = """
version: 1.0

description: A workflow for testing with items and concurrency.

input:
  - values

tasks:
  task1:
    with:
      items: <% ctx().values %>
    action: core.local
    input:
      cmd: echo <% item() %>
    next:
      - when: <% succeeded() %>
        do:
          - task2

  task2:
    action: core.local
    input:
      cmd: "echo hello "

        """
        spec_yaml = """
---
  workflow: "samplewith.yaml"
  inputs: {"values":["1","2","3"]}
  routes: [[]]
  expected_workflow_status: failed
  task_sequence:
    - task1:
        route: 0
        status: ["succeeded","failed","failed"]

        """

        def do_run(spec_y):
            spec = TestCaseSpec(spec_y, "fixture")
            workflow_path = "/tmp"
            fixture = WorkflowTestFixture(spec, workflow_path)
            fixture.run_test()

        mock_open = mock.mock_open(read_data=wf)
        if sys.version_info[0] < 3:
            with mock.patch("__builtin__.open", mock_open):
                do_run(spec_yaml)
        else:
            with mock.patch("builtins.open", mock_open):
                do_run(spec_yaml)

    def test_with_items_success(self):
        wf = """
version: 1.0

description: A workflow for testing with items and concurrency.

input:
  - values

tasks:
  task1:
    with:
      items: <% ctx().values %>
    action: core.local
    input:
      cmd: echo <% item() %>
    next:
      - when: <% succeeded() %>
        do:
          - task2

  task2:
    action: core.local
    input:
      cmd: "echo hello "

        """
        spec_yaml = """
---
  workflow: "samplewith.yaml"
  inputs: {"values":["1","2","3"]}
  routes: [[]]
  task_sequence:
    - task1:
        route: 0
        result: [{"result":{"result":1}},{"result":{"result":2}},{"result":{"result":1}}]
        status: ["succeeded","succeeded","succeeded"]
    - task2:
        route: 0
        status: "succeeded"

        """

        def do_run(spec_y):
            spec = TestCaseSpec(spec_y, "fixture")
            workflow_path = "/tmp"
            fixture = WorkflowTestFixture(spec, workflow_path)
            fixture.run_test()

        mock_open = mock.mock_open(read_data=wf)
        if sys.version_info[0] < 3:
            with mock.patch("__builtin__.open", mock_open):
                do_run(spec_yaml)
        else:
            with mock.patch("builtins.open", mock_open):
                do_run(spec_yaml)

    def test_load_workflow_spec_results(self):

        wf = """
                version: 1.0
                description: A basic sequential workflow.
                tasks:
                  task1:
                    action: core.echo message=hello
                    next:
                      - when: <% succeeded() and result().result.output = 1 %>
                        publish:
                          - greeting: 1

        """
        spec_yaml = """
        workflow: "/tmp/test"
        routes: [[]]
        task_sequence:
          - task1:
              route: 0
              status: "succeeded"
              result: {"result":{"output": 1}}
          - continue:
              route: 0
              status: succeeded
        """

        def do_run(spec_y):
            spec = TestCaseSpec(spec_y, "fixture")
            workflow_path = "/tmp"
            fixture = WorkflowTestFixture(spec, workflow_path)
            fixture.run_test()

        mock_open = mock.mock_open(read_data=wf)
        if sys.version_info[0] < 3:
            with mock.patch("__builtin__.open", mock_open):
                do_run(spec_yaml)
        else:
            with mock.patch("builtins.open", mock_open):
                do_run(spec_yaml)

    def test_load_workflow_spec_throws(self):

        wf = """
                version: 1.0

                description: A basic sequential workflow.

                input:
                  - name

                vars:
                  - greeting: null

                output:
                  - greeting: <% ctx().greeting %>

                tasks:
                  task1:
                    action: core.echo message=<% ctx().name %>
                    next:
                      - when: <% succeeded() %>
                        publish: greeting=<% ctx(st2).test %>
        """
        spec_yaml = """
        workflow: "/tmp/test"
        routes: [[]]
        task_sequence:
          - task1:
              route: 0
              status: "succeeded"
          - task2:
              route: 0
              status: "succeeded"
          - task3:
              route: 0
              status: "failed"
          - continue:
              route: 0
              status: "succeeded"
          - exception:
              route: 0
              status: "succeeded"
        """
        mock_open = mock.mock_open(read_data=wf)
        if sys.version_info[0] < 3:
            with mock.patch("__builtin__.open", mock_open):
                spec = TestCaseSpec(spec_yaml, "fixture")
                workflow_path = "/tmp"
                self.assertRaises(exc.WorkflowSpecError, WorkflowTestFixture, spec, workflow_path)
        else:
            with mock.patch("builtins.open", mock_open):
                spec = TestCaseSpec(spec_yaml, "fixture")
                workflow_path = "/tmp"
                self.assertRaises(exc.WorkflowSpecError, WorkflowTestFixture, spec, workflow_path)

    @mock.patch("orquesta.tests.mocks.WorkflowTestFixture.load_wf_spec")
    def test_run_test_throw(self, load_wf_spec):

        wf_name = "sequential"
        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)

        load_wf_spec.return_value = wf_spec
        spec_yaml = """
        workflow: "/tmp/test"
        routes: [[]]
        task_sequence:
          - task1:
              route: 0
              status: "succeeded"
              result: {}
          - task2:
              route: 0
              status: "succeeded"
              result: {}
          - task3:
              route: 0
              status: "failed"
              result: {}
          - continue:
              route: 0
              status: "succeeded"
              result: {}
          - exception:
              route: 0
              status: "succeeded"
              result: {}
        """

        spec = TestCaseSpec(spec_yaml, "fixture")
        workflow_path = "/tmp"
        fixture = WorkflowTestFixture(spec, workflow_path)
        self.assertRaises(exc.MockConductorTaskSequenceError, fixture.run_test)

    @mock.patch("orquesta.tests.mocks.WorkflowTestFixture.load_wf_spec")
    def test_run_test(self, load_wf_spec):

        wf_name = "sequential"
        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)
        load_wf_spec.return_value = wf_spec
        spec_yaml = """
        workflow: "/tmp/test"
        routes: [[]]
        task_sequence:
          - task1:
              route: 0
              status: "succeeded"
              result: {"result":{"test":true}}
          - task2:
              route: 0
              status: "succeeded"
              result: {"result":{"test":true}}
          - task3:
              route: 0
              status: "succeeded"
              result: {"result":{"test":true}}
          - continue:
              route: 0
              status: "succeeded"
              result: {"result":{"test":true}}
        """

        spec = TestCaseSpec(spec_yaml, "fixture")
        workflow_path = "/tmp"
        fixture = WorkflowTestFixture(spec, workflow_path)
        fixture.run_test()


if __name__ == "__main__":
    unittest.main()
