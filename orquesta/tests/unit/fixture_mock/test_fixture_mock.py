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

import orquesta.exceptions as exc
from orquesta.fixture_mock import Fixture
from orquesta.specs.mock.models import TestFileSpec
from orquesta.tests.unit.conducting.native import base


class FixtureTest(base.OrchestraWorkflowConductorTest):

    @mock.patch("orquesta.fixture_mock.Fixture.load_wf_spec")
    def test_run_test_throw(self, load_wf_spec):

        wf_name = "sequential"
        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)

        load_wf_spec.return_value = wf_spec
        spec_yaml = """
        file: "/tmp/test"
        expected_task_seq:
            - "task1"
            - "task2"
            - "task3"
            - "continue"
            - "exception"
        """
        spec = TestFileSpec(spec_yaml, "fixture")
        fixture = Fixture(spec)
        self.assertRaises(exc.TaskEquality, fixture.run_test)

    @mock.patch("orquesta.fixture_mock.Fixture.load_wf_spec")
    def test_run_test(self, load_wf_spec):

        wf_name = "sequential"
        self.assert_spec_inspection(wf_name)
        wf_def = self.get_wf_def(wf_name)
        wf_spec = self.spec_module.instantiate(wf_def)

        load_wf_spec.return_value = wf_spec
        spec_yaml = """
        file: "/tmp/test"
        expected_task_seq:
            - "task1"
            - "task2"
            - "task3"
            - "continue"
        """
        spec = TestFileSpec(spec_yaml, "fixture")
        fixture = Fixture(spec)
        fixture.run_test()
