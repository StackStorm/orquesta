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

from six.moves import queue
import unittest

from orquesta import conducting
from orquesta import events
from orquesta import exceptions as exc
from orquesta.specs import loader as spec_loader
from orquesta import statuses


class MockActionExecution(object):
    def __init__(
        self,
        task_id,
        status=statuses.SUCCEEDED,
        result=None,
        item_id=None,
        seq_id=None,
        num_iter=1,
        iter_idx=0,
    ):
        self.task_id = task_id
        self.status = status
        self.result = result
        self.item_id = item_id
        self.seq_id = seq_id
        self.num_iter = num_iter
        self.iter_idx = iter_idx
        self.iter_pos = iter_idx - 1

        if self.iter_idx < 0:
            raise exc.WorkflowRehearsalError(
                "The zero based value of iter_idx for MockActionExecution "
                "must be greater than or equal to zero."
            )

        if self.num_iter < 1:
            raise exc.WorkflowRehearsalError(
                "The value of num_iter for MockActionExecution must be greater than zero."
            )


class WorkflowTestCaseMixin(object):
    def get_mock_action_execution(self, task_id, item_id=None, seq_id=None):
        ac_exs = [
            x
            for x in self.mock_action_executions
            if x.task_id == task_id and x.iter_pos < x.iter_idx + x.num_iter - 1
        ]

        if ac_exs and seq_id is not None:
            ac_exs = [x for x in ac_exs if x.seq_id == seq_id]

        if ac_exs and item_id is not None:
            ac_exs = [x for x in ac_exs if x.item_id == item_id]

        if len(ac_exs) > 0 and ac_exs[0].seq_id is not None and seq_id is None:
            return None

        return ac_exs[0] if len(ac_exs) > 0 else None


class WorkflowTestCase(WorkflowTestCaseMixin):
    def __init__(
        self,
        wf_def,
        expected_task_seq,
        inputs=None,
        spec_module_name="native",
        mock_action_executions=None,
        expected_inspection_errors=None,
        expected_routes=None,
        expected_workflow_status=None,
        expected_errors=None,
        expected_output=None,
        expected_term_tasks=None,
    ):
        self.wf_def = wf_def
        self.expected_task_seq = expected_task_seq
        self.inputs = inputs
        self.spec_module_name = spec_module_name
        self.mock_action_executions = mock_action_executions
        self.expected_inspection_errors = expected_inspection_errors
        self.expected_routes = expected_routes
        self.expected_workflow_status = expected_workflow_status
        self.expected_errors = expected_errors
        self.expected_output = expected_output
        self.expected_term_tasks = expected_term_tasks

        if not self.mock_action_executions:
            self.mock_action_executions = []

        if not self.expected_inspection_errors:
            self.expected_inspection_errors = {}

        if not self.expected_routes:
            self.expected_routes = [[]]

        if self.inputs is None:
            self.inputs = {}

        if self.expected_workflow_status is None:
            self.expected_workflow_status = statuses.SUCCEEDED


class WorkflowRerunTestCase(WorkflowTestCaseMixin):
    def __init__(
        self,
        conductor,
        expected_task_seq,
        rerun_tasks=None,
        mock_action_executions=None,
        expected_inspection_errors=None,
        expected_routes=None,
        expected_workflow_status=None,
        expected_errors=None,
        expected_output=None,
        expected_term_tasks=None,
    ):
        self.conductor = conductor
        self.expected_task_seq = expected_task_seq
        self.rerun_tasks = rerun_tasks
        self.mock_action_executions = mock_action_executions
        self.expected_inspection_errors = expected_inspection_errors
        self.expected_routes = expected_routes
        self.expected_workflow_status = expected_workflow_status
        self.expected_errors = expected_errors
        self.expected_output = expected_output
        self.expected_term_tasks = expected_term_tasks

        if not self.mock_action_executions:
            self.mock_action_executions = []

        if not self.expected_inspection_errors:
            self.expected_inspection_errors = {}

        if not self.expected_routes:
            self.expected_routes = [[]]

        if self.expected_workflow_status is None:
            self.expected_workflow_status = statuses.SUCCEEDED


class WorkflowRehearsal(unittest.TestCase):
    def __init__(self, session, *args, **kwargs):
        super(WorkflowRehearsal, self).__init__(*args, **kwargs)

        if not session:
            raise exc.WorkflowRehearsalError("The session object is not provided.")

        if not isinstance(session, WorkflowTestCase) and not isinstance(
            session, WorkflowRerunTestCase
        ):
            raise exc.WorkflowRehearsalError(
                "The session object is not type of WorkflowTestCase or WorkflowRerunTestCase."
            )

        self.session = session
        self.rerun = False

        if isinstance(session, WorkflowTestCase):
            self.spec_module = spec_loader.get_spec_module(session.spec_module_name)
            self.wf_spec = self.spec_module.instantiate(self.session.wf_def)
            self.inspection_errors = {}
            self.conductor = None
        elif isinstance(session, WorkflowRerunTestCase):
            self.conductor = self.session.conductor
            self.spec_module = self.conductor.spec_module
            self.wf_spec = self.conductor.spec
            self.inspection_errors = {}
            self.rerun = True

        for mock_ac_ex in self.session.mock_action_executions:
            if not self.wf_spec.tasks.has_task(mock_ac_ex.task_id):
                raise exc.InvalidTask(mock_ac_ex.task_id)

            task_spec = self.wf_spec.tasks.get_task(mock_ac_ex.task_id)

            if task_spec.has_items() and mock_ac_ex.item_id is None:
                msg = 'Mock action execution for with items task "%s" is misssing "item_id".'
                raise exc.WorkflowRehearsalError(msg % mock_ac_ex.task_id)

    def runTest(self):
        """Override runTest

        The WorkflowRehearsal will be called directly in other unit tests. In python 2.7, doing
        this will lead to error "no such test method runTest". Since WorkflowRehearsal uses
        unittest.TestCase function and does not implement any unit tests, we can override and
        pass runTest here.
        """
        pass

    def assert_spec_inspection(self):
        self.inspection_errors = self.wf_spec.inspect()
        self.assertDictEqual(self.inspection_errors, self.session.expected_inspection_errors)

    def assert_conducting_sequences(self):
        run_q = queue.Queue()
        items_task_accum_result = {}

        # Inspect workflow spec and check for errors.
        self.assert_spec_inspection()

        # If there is expected inspection errors, then skip the check for conducting sequences.
        if self.session.expected_inspection_errors:
            return

        if not self.rerun:
            # Instantiate a workflow conductor to check conducting sequences.
            self.conductor = conducting.WorkflowConductor(self.wf_spec, inputs=self.session.inputs)
            self.conductor.request_workflow_status(statuses.RUNNING)
        else:
            # Request workflow rerun and assert workflow status is running.
            self.conductor.request_workflow_rerun(task_requests=self.session.rerun_tasks)
            self.assertEqual(self.conductor.get_workflow_status(), statuses.RESUMING)

        # Get start tasks and being conducting workflow.
        for task in self.conductor.get_next_tasks():
            run_q.put(task)

        # Serialize workflow conductor to mock async execution.
        wf_conducting_state = self.conductor.serialize()

        # Process until there are not more tasks in queue.
        while not run_q.empty():
            # Deserialize workflow conductor to mock async execution.
            self.conductor = conducting.WorkflowConductor.deserialize(wf_conducting_state)

            # Process all the tasks in the run queue.
            while not run_q.empty():
                current_task = run_q.get()
                current_task_id = current_task["id"]
                current_task_route = current_task["route"]
                current_task_spec = self.wf_spec.tasks.get_task(current_task_id)

                # Set task status to running.
                ac_ex_event = events.ActionExecutionEvent(statuses.RUNNING)
                self.conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)

                # Mock completion of the task and apply mock action execution if given.
                current_seq_id = len(self.conductor.workflow_state.sequence) - 1

                ac_ex = self.session.get_mock_action_execution(
                    current_task_id, seq_id=current_seq_id
                )

                if not ac_ex:
                    ac_ex = self.session.get_mock_action_execution(current_task_id)

                if not ac_ex:
                    ac_ex = MockActionExecution(current_task_id)
                else:
                    ac_ex.iter_pos += 1

                if not current_task_spec.has_items():
                    ac_ex_event = events.ActionExecutionEvent(ac_ex.status, result=ac_ex.result)
                else:
                    if current_task_id not in items_task_accum_result:
                        items_task_accum_result[current_task_id] = []

                    len_accum_result = len(items_task_accum_result[current_task_id])

                    if ac_ex.item_id is None:
                        ac_ex.item_id = len_accum_result

                    if ac_ex.item_id > len_accum_result - 1:
                        placeholders = [None] * (ac_ex.item_id - len_accum_result + 1)
                        items_task_accum_result[current_task_id].extend(placeholders)

                    items_task_accum_result[current_task_id][ac_ex.item_id] = ac_ex.result

                    ac_ex_event = events.TaskItemActionExecutionEvent(
                        ac_ex.item_id,
                        ac_ex.status,
                        result=ac_ex.result,
                        accumulated_result=items_task_accum_result[current_task_id],
                    )

                self.conductor.update_task_state(current_task_id, current_task_route, ac_ex_event)

            # Identify the next set of tasks.
            for next_task in self.conductor.get_next_tasks():
                run_q.put(next_task)

            # Serialize workflow execution graph to mock async execution.
            wf_conducting_state = self.conductor.serialize()

        actual_task_seq = [
            (entry["id"], entry["route"]) for entry in self.conductor.workflow_state.sequence
        ]

        expected_task_seq = [
            task_seq if isinstance(task_seq, tuple) else (task_seq, 0)
            for task_seq in self.session.expected_task_seq
        ]

        self.assertListEqual(actual_task_seq, expected_task_seq)
        self.assertListEqual(self.conductor.workflow_state.routes, self.session.expected_routes)

        if self.conductor.get_workflow_status() in statuses.COMPLETED_STATUSES:
            self.conductor.render_workflow_output()

        self.assertEqual(
            self.conductor.get_workflow_status(), self.session.expected_workflow_status
        )

        if self.session.expected_errors is not None:
            self.assertListEqual(self.conductor.errors, self.session.expected_errors)

        if self.session.expected_output is not None:
            self.assertDictEqual(self.conductor.get_workflow_output(), self.session.expected_output)

        if self.session.expected_term_tasks:
            expected_term_tasks = [
                (task, 0) if not isinstance(task, tuple) else task
                for task in self.session.expected_term_tasks
            ]

            term_tasks = self.conductor.workflow_state.get_terminal_tasks()
            actual_term_tasks = [(t["id"], t["route"]) for i, t in term_tasks]
            expected_term_tasks = sorted(expected_term_tasks, key=lambda x: x[0])
            actual_term_tasks = sorted(actual_term_tasks, key=lambda x: x[0])
            self.assertListEqual(actual_term_tasks, expected_term_tasks)
