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
from itertools import chain
import json
import logging
import os
from pprint import pformat
import yaml

from six.moves import queue

from orquesta import conducting
from orquesta import events
from orquesta.specs import loader as spec_loader
from orquesta.specs.mock.models import TestFileSpec
from orquesta.specs.native.v1.models import WorkflowSpec
from orquesta import statuses
from orquesta.tests import exceptions as exc


LOG = logging.getLogger(__name__)

FIXTURE_EXTS = {".json": json.load, ".yml": yaml.safe_load, ".yaml": yaml.safe_load}


class WorkflowConductorMock(object):

    """read fixture for workflow yaml, inputs, and what to check.

    """

    spec_module_name = "mock"

    def __init__(
        self,
        wf_spec,
        expected_task_seq,
        expected_routes=None,
        inputs=None,
        mock_statuses=None,
        mock_results=None,
        expected_workflow_status=None,
        expected_output=None,
        expected_term_tasks=None,
    ):

        """accepts a workflow spec and different results to check

        :param wf_spec: WorkflowSpec, str
        :param expected_task_seq: list
        :param expected_routes: list
        :param inputs: list
        :param mock_statuses: list
        :param mock_results: list
        :param expected_workflow_status: list
        :param expected_output:
        :param expected_term_tasks: list
        """

        if isinstance(wf_spec, str):
            # load spec from file
            wf_def = self.get_fixture_content(wf_spec)
            spec_module = spec_loader.get_spec_module(WorkflowConductorMock.spec_module_name)
            wf_spec = spec_module.instantiate(wf_def)

        self.wf_spec = wf_spec
        self.expected_task_seq = expected_task_seq
        self.expected_routes = expected_routes
        self.inputs = inputs
        self.mock_statuses = mock_statuses
        self.mock_results = mock_results
        self.expected_workflow_status = expected_workflow_status
        self.expected_output = expected_output
        self.expected_term_tasks = expected_term_tasks

        if not self.expected_routes:
            self.expected_routes = [[]]

        if self.inputs is None:
            self.inputs = {}

        self.run_q = queue.Queue()
        self.status_q = queue.Queue()
        self.result_q = queue.Queue()

        if self.mock_statuses:
            for item in mock_statuses:
                self.status_q.put(item)

        if self.mock_results:
            for item in mock_results:
                self.result_q.put(item)

    def assert_conducting_sequences(self):
        conductor = conducting.WorkflowConductor(self.wf_spec, inputs=self.inputs)
        conductor.request_workflow_status(statuses.RUNNING)

        # Get start tasks and being conducting workflow.
        for task in conductor.get_next_tasks():
            self.run_q.put(task)

        # Serialize workflow conductor to mock async execution.
        wf_conducting_state = conductor.serialize()

        # Process until there are not more tasks in queue.
        while not self.run_q.empty():
            # Deserialize workflow conductor to mock async execution.
            conductor = conducting.WorkflowConductor.deserialize(wf_conducting_state)
            # Process all the tasks in the run queue.
            while not self.run_q.empty():
                current_task = self.run_q.get()
                current_task_id = current_task["id"]
                current_task_route = current_task["route"]
                # mock running
                for action in current_task["actions"]:
                    ac_ex_event = None
                    if "item_id" not in action or action["item_id"] is None:
                        ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
                    else:
                        msg = 'Mark task "%s", route "%s", item "%s" in conductor as running.'
                        msg = msg % (task["id"], str(task["route"]), action["item_id"])
                        LOG.debug(msg)
                        ac_ex_event = events.TaskItemActionExecutionEvent(
                            action["item_id"], statuses.RUNNING
                        )

                    conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)

                # Mock completion of the task.
                status = self.status_q.get() if not self.status_q.empty() else statuses.SUCCEEDED
                result = self.result_q.get() if not self.result_q.empty() else None
                for index, action in enumerate(current_task["actions"]):
                    if "item_id" not in action or action["item_id"] is None:
                        ac_ex_event = events.ActionExecutionEvent(status, result=result)
                    else:
                        result = [] if result is None else result
                        if len(result) > 0:
                            accumulated_result = [
                                item.get("result") if item else None for item in result[: index + 1]
                            ]
                        else:
                            accumulated_result = [None for i in range(index + 1)]

                        ac_ex_event = events.TaskItemActionExecutionEvent(
                            action["item_id"], status[index], result=accumulated_result
                        )
                    entry = conductor.update_task_state(
                        current_task_id, current_task_route, ac_ex_event
                    )
                    LOG.debug("current task id: %s" % entry["id"])
                    LOG.debug("route: %s" % entry["route"])
                    LOG.debug("in context: %s" % conductor.get_task_context(entry["ctxs"]["in"]))
                    for k, v in entry.get("ctxs", {}).get("out", {}).items():
                        LOG.debug("out %s: %s" % (k, conductor.get_task_context([v])))

            # Identify the next set of tasks.
            for next_task in conductor.get_next_tasks():
                self.run_q.put(next_task)

            # Serialize workflow execution graph to mock async execution.
            wf_conducting_state = conductor.serialize()

        actual_task_seq = [
            [entry["id"], entry["route"]] for entry in conductor.workflow_state.sequence
        ]
        LOG.debug(" routes: %s", str(conductor.workflow_state.routes))
        expected_task_seq = [
            list(task_seq)
            if isinstance(task_seq, tuple) or isinstance(task_seq, list)
            else [task_seq, 0]
            for task_seq in self.expected_task_seq
        ]

        if len(conductor.errors) > 0:
            # fail in workflow. These are not syntax errors since those are
            # checked via inpect
            # do not raise exception here because the unit test could be testing
            # for a failure condition itself.
            LOG.error(conductor.errors)

        if actual_task_seq != expected_task_seq:
            LOG.error("Actual task Seq  : %s", str(actual_task_seq))
            LOG.error("Expected Task Seq: %s", str(expected_task_seq))
            raise exc.MockConductorTaskSequenceError

        if conductor.workflow_state.routes != self.expected_routes:
            LOG.error("Actual routes  : %s", str(conductor.workflow_state.routes))
            LOG.error("Expected routes: %s", str(self.expected_routes))
            raise exc.MockConductorTaskRouteError

        if conductor.get_workflow_status() in statuses.COMPLETED_STATUSES:
            conductor.render_workflow_output()

        if self.expected_workflow_status is None:
            self.expected_workflow_status = statuses.SUCCEEDED

        if conductor.get_workflow_status() != self.expected_workflow_status:
            LOG.error("Actual workflow status  : %s", conductor.get_workflow_status())
            LOG.error("Expected workflow status: %s", str(self.expected_workflow_status))
            raise exc.MockConductorWorkflowStatusError

        if self.expected_output is not None:
            if conductor.get_workflow_output() != self.expected_output:
                LOG.error("Actual workflow output  : %s", str(conductor.get_workflow_output()))
                LOG.error("Expected workflow output: %s", str(self.expected_output))
                raise exc.MockConductorWorkflowOutputError

        if self.expected_term_tasks:
            expected_term_tasks = [
                (task, 0) if not isinstance(task, tuple) else task
                for task in self.expected_term_tasks
            ]

            term_tasks = conductor.workflow_state.get_terminal_tasks()
            actual_term_tasks = [(t["id"], t["route"]) for i, t in term_tasks]
            expected_term_tasks = sorted(expected_term_tasks, key=lambda x: x[0])
            actual_term_tasks = sorted(actual_term_tasks, key=lambda x: x[0])
            if actual_term_tasks != expected_term_tasks:
                LOG.error("Actual term tasks : %s", str(actual_term_tasks))
                LOG.error("Expected term tasks: %s", str(expected_term_tasks))
                raise exc.MockConductorWorkflowTermsError

        return conductor

    @staticmethod
    def get_fixture_content(file_path, raw=False):

        """get fixture contents from file path

        :param file_path: str
        :param raw: boolean
        """

        file_name, file_ext = os.path.splitext(file_path)

        if file_ext not in FIXTURE_EXTS.keys():
            raise Exception("Unsupported fixture file ext type of %s." % file_ext)

        with open(file_path, "r") as fd:
            return FIXTURE_EXTS[file_ext](fd) if not raw else fd.read()


class WorkflowTestFixture(object):
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
        self.full_workflow_path = os.path.join(self.workflow_path, self.fixture_spec.workflow)
        self.workflow_spec = self.load_wf_spec(self.full_workflow_path)
        self.task_sequence = self._get_task_sequence()
        self.mock_statuses = self._get_mock_statuses()
        self.mock_results = self._get_mock_results()

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

    def _get_mock_statuses(self):
        """retrieve task statuses

        """
        return [
            value.get("status", "succeeded")
            for key, value in chain.from_iterable(
                [i.items() for i in self.fixture_spec.task_sequence]
            )
        ]

    def _get_task_sequence(self):
        """retrieve task seq tuples from fixture

        """
        return [
            (key, value.get("route", 0))
            for key, value in chain.from_iterable(
                [i.items() for i in self.fixture_spec.task_sequence]
            )
        ]

    def _get_mock_results(self):
        """retrieve results from fixture

        """
        return [
            value.get("result", {})
            for key, value in chain.from_iterable(
                [i.items() for i in self.fixture_spec.task_sequence]
            )
        ]

    def run_test(self):
        """read fixture spec file

        perform test of spec against all expected properties

        :return: list[str]
        """
        expected_task_seq = self.task_sequence
        expected_output = self.fixture_spec.expected_output
        inputs = self.fixture_spec.inputs
        expected_routes = self.fixture_spec.routes
        mock_statuses = self.mock_statuses
        mock_results = self.mock_results
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
    parser.add_argument("--loglevel", type=str, help="set logging level", default="info")

    # mutualy exclusive group
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", "--fixture", type=str, help="fixture file ")
    group.add_argument("-d", "--fixture_dir", type=str, help="fixture directory")

    parser.add_argument(
        "-p", "--workflow_path", type=str, help="path to workflow file", required=True
    )
    args = parser.parse_args()
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % args.loglevel)

    LOG.setLevel(numeric_level)
    handler = logging.StreamHandler()
    handler.setLevel(numeric_level)
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    LOG.addHandler(handler)
    # single file fixture
    if args.fixture is not None:
        fixture = WorkflowTestFixture.load_from_file(args.workflow_path, args.fixture, True)
        fixture.run_test()
        print(args.workflow_path + "/" + fixture.fixture_spec.workflow + " test successful")
    # directory of fixtures
    LINE_DASH = 20
    if args.fixture_dir is not None:
        errors = []
        root, dirs, files = next(os.walk(args.fixture_dir))
        LOG.info("-" * LINE_DASH)
        LOG.info("-" * LINE_DASH)
        for filename in files:
            full_path = os.path.join(root, filename)
            try:
                LOG.info("testing %s", filename)
                fixture = WorkflowTestFixture.load_from_file(args.workflow_path, full_path, True)
                fixture.run_test()
                LOG.info(
                    "%s/%s test successful", args.workflow_path, fixture.fixture_spec.workflow,
                )
                LOG.info("-" * LINE_DASH)
            except exc.OrquestaException as e:
                LOG.error("Error: ")
                LOG.error(pformat(e))
                errors.append([filename, e])
        if len(errors) > 0:
            LOG.error("-" * LINE_DASH)
            LOG.error("-" * LINE_DASH)
            LOG.error("Errors Found during testing")
            for e in errors:
                LOG.error("-" * LINE_DASH)
                LOG.error("fixture: %s", e[0])
                LOG.error("exception: %s", pformat(e[1]))
            raise exc.OrquestaFixtureTestError
