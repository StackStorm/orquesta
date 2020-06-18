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

import argparse
import logging
import os
from pprint import pformat

from orquesta.conduct_mock import WorkflowConductorMock
import orquesta.exceptions as exc
from orquesta.specs.mock.models import TestFileSpec
from orquesta.specs.native.v1.models import WorkflowSpec

LOG = logging.getLogger(__name__)


class Fixture(object):
    def __init__(self, spec, workflow_path, pprint=False):
        """Fixture for testing workflow

        :param spec: TestFileSpec
        :param workflow_spec: str - directory containing file named in
        fixture
        :param cmd: boolean - True to prettyprint errors
        """
        self.workflow_path = workflow_path
        self.fixture_spec = spec
        self.pprint = pprint
        if not isinstance(spec, TestFileSpec):
            raise exc.IncorrectSpec
        errors = self.fixture_spec.inspect()
        if len(errors) > 0:
            if self.pprint:
                LOG.error(pformat(errors))
            else:
                LOG.error(errors)
            raise exc.FixtureMockSpecError
        self.full_workflow_path = os.path.join(
            self.workflow_path, self.fixture_spec.file
        )
        self.workflow_spec = self.load_wf_spec(self.full_workflow_path)

    @classmethod
    def load_from_file(cls, workflow_path, fixture_filename, pprint):
        with open(fixture_filename, "r") as f:
            fixture_spec = TestFileSpec(f.read(), "")
            return cls(fixture_spec, workflow_path, pprint)

    def load_wf_spec(self, input_file):
        """load a workflow spec from a file

        :param input_file: str
        """
        with open(input_file, "r") as f:
            workflow_def = f.read()
            wf_spec = WorkflowSpec(workflow_def, "native")
            errors = wf_spec.inspect()
            if len(errors) > 0:
                if self.pprint:
                    LOG.error(pformat(errors))
                else:
                    LOG.error(errors)
                raise exc.WorkflowSpecError
            return wf_spec

    def run_test(self):
        """read fixture spec file

        perform test of spec against all expected properties

        :return: list[str]
        """
        expected_task_seq = self.fixture_spec.expected_task_seq
        expected_output = self.fixture_spec.expected_output
        inputs = self.fixture_spec.inputs
        expected_routes = self.fixture_spec.expected_routes
        mock_statuses = self.fixture_spec.mock_statuses
        mock_results = self.fixture_spec.mock_results
        expected_workflow_status = self.fixture_spec.expected_workflow_status
        expected_output = self.fixture_spec.expected_output
        expected_term_tasks = self.fixture_spec.expected_term_tasks

        conductor = WorkflowConductorMock(
            self.workflow_spec,
            expected_task_seq,
            inputs=inputs,
            expected_routes=expected_routes,
            mock_statuses=mock_statuses,
            mock_results=mock_results,
            expected_workflow_status=expected_workflow_status,
            expected_output=expected_output,
            expected_term_tasks=expected_term_tasks,
        )
        conductor.assert_conducting_sequences()

def main():

    parser = argparse.ArgumentParser("Stackstorm Workflow Testing")
    parser.add_argument(
        "--loglevel", type=str, help="set logging level", default="info"
    )
    parser.add_argument(
        "-f", "--fixture", type=str, help="fixture file ", required=True
    )
    parser.add_argument(
        "-p", "--workflow_path", type=str, help="path to workflow file", required=True
    )
    args = parser.parse_args()
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % args.loglevel)
    logging.basicConfig(level=numeric_level)
    fixture = Fixture.load_from_file(args.workflow_path, args.fixture, True)
    fixture.run_test()
    print(args.fixture + " test successful")


if __name__ == "__main__":
    main()
