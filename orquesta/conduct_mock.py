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

import json
import logging
import os
import yaml

from six.moves import queue

from orquesta import conducting
from orquesta import events
from orquesta import exceptions as exc
from orquesta.specs import loader as spec_loader
from orquesta import statuses


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

                # Set task status to running.
                ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
                conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)

                # Mock completion of the task.
                status = self.status_q.get() if not self.status_q.empty() else statuses.SUCCEEDED
                result = self.result_q.get() if not self.result_q.empty() else None

                ac_ex_event = events.ActionExecutionEvent(status, result=result)
                conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)
            # Identify the next set of tasks.
            for next_task in conductor.get_next_tasks():
                self.run_q.put(next_task)

            # Serialize workflow execution graph to mock async execution.
            wf_conducting_state = conductor.serialize()

        actual_task_seq = [
            [entry["id"], entry["route"]] for entry in conductor.workflow_state.sequence
        ]

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
            LOG.info(conductor.errors)

        if actual_task_seq != expected_task_seq:
            LOG.error("Actual task Seq  : %s", str(actual_task_seq))
            LOG.error("Expected Task Seq: %s", str(expected_task_seq))
            raise exc.TaskEquality

        if conductor.workflow_state.routes != self.expected_routes:
            LOG.error("Actual routes  : %s", str(conductor.workflow_state.routes))
            LOG.error("Expected routes: %s", str(self.expected_routes))
            raise exc.RouteEquality

        if conductor.get_workflow_status() in statuses.COMPLETED_STATUSES:
            conductor.render_workflow_output()

        if self.expected_workflow_status is None:
            self.expected_workflow_status = statuses.SUCCEEDED

        if conductor.get_workflow_status() != self.expected_workflow_status:
            LOG.error("Actual workflow status  : %s", conductor.get_workflow_status())
            LOG.error("Expected workflow status: %s", str(self.expected_workflow_status))
            raise exc.StatusEquality

        if self.expected_output is not None:
            if conductor.get_workflow_output() != self.expected_output:
                LOG.error("Actual workflow output  : %s", str(conductor.get_workflow_output()))
                LOG.error("Expected workflow output: %s", str(self.expected_output))
                raise exc.OutputEquality

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
                raise exc.TermsEquality

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
