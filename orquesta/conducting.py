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

import logging
import six

from six.moves import queue

from orquesta import constants
from orquesta import events
from orquesta import exceptions as exc
from orquesta.expressions import base as expr_base
from orquesta import graphing
from orquesta import machines
from orquesta.specs import base as spec_base
from orquesta.specs import loader as spec_loader
from orquesta import statuses
from orquesta.utils import context as ctx_util
from orquesta.utils import dictionary as dict_util
from orquesta.utils import jsonify as json_util
from orquesta.utils import plugin as plugin_util


LOG = logging.getLogger(__name__)


class WorkflowState(object):
    def __init__(self, conductor=None):
        self.conductor = conductor
        self.contexts = list()
        self.routes = list()
        self.sequence = list()
        self.staged = list()
        self.status = statuses.UNSET
        self.tasks = dict()
        self.reruns = list()

    def serialize(self):
        data = {
            "contexts": json_util.deepcopy(self.contexts),
            "routes": json_util.deepcopy(self.routes),
            "sequence": json_util.deepcopy(self.sequence),
            "staged": json_util.deepcopy(self.staged),
            "status": self.status,
            "tasks": json_util.deepcopy(self.tasks),
        }

        if self.reruns:
            data["reruns"] = json_util.deepcopy(self.reruns)

        return data

    @classmethod
    def deserialize(cls, data):
        instance = cls()
        instance.contexts = json_util.deepcopy(data.get("contexts", list()))
        instance.routes = json_util.deepcopy(data.get("routes", list()))
        instance.sequence = json_util.deepcopy(data.get("sequence", list()))
        instance.staged = json_util.deepcopy(data.get("staged", list()))
        instance.status = data.get("status", statuses.UNSET)
        instance.tasks = json_util.deepcopy(data.get("tasks", dict()))
        instance.reruns = json_util.deepcopy(data.get("reruns", list()))

        return instance

    def has_task(self, task_id, route):
        return constants.TASK_STATE_ROUTE_FORMAT % (task_id, str(route)) in self.tasks

    def get_task(self, task_id, route):
        return self.sequence[self.tasks[constants.TASK_STATE_ROUTE_FORMAT % (task_id, str(route))]]

    def get_tasks(self, task_id=None, route=None, last_occurrence=True):
        if task_id and route is None:
            result = [(i, t) for i, t in enumerate(self.sequence) if t["id"] == task_id]
        elif task_id and route is not None:
            result = [
                (i, t)
                for i, t in enumerate(self.sequence)
                if t["id"] == task_id and t["route"] == route
            ]
        else:
            result = list(enumerate(self.sequence))

        if last_occurrence:
            result = [s for s in result if s[0] in self.tasks.values()]

        return result

    def get_tasks_by_status(self, statuses, last_occurrence=True):
        result = [
            (i, t) for i, t in enumerate(self.sequence) if "status" in t and t["status"] in statuses
        ]

        if last_occurrence:
            result = [s for s in result if s[0] in self.tasks.values()]

        return result

    def get_task_sequence(self, task_id, route):
        idx = self.tasks[constants.TASK_STATE_ROUTE_FORMAT % (task_id, str(route))]
        seq = [(idx, self.sequence[idx])]

        q = queue.Queue()
        q.put((task_id, route))

        while not q.empty():
            task_id, route = q.get()

            for i, t in enumerate(self.sequence):
                for k, v in six.iteritems(t["prev"]):
                    p = self.sequence[v]
                    if p["id"] == task_id and p["route"] == route:
                        seq.append((i, t))
                        if (i, t) not in seq:
                            q.put((t["id"], t["route"]))

        return seq

    def get_terminal_tasks(self):
        return [(i, t) for i, t in enumerate(self.sequence) if t.get("term", False)]

    def has_next_tasks(self, task_id=None, route=None):
        return self.conductor.has_next_tasks(task_id=task_id, route=route)

    @property
    def has_active_tasks(self):
        return len(self.get_tasks_by_status(statuses.ACTIVE_STATUSES)) > 0

    @property
    def has_pausing_tasks(self):
        return len(self.get_tasks_by_status([statuses.PAUSING])) > 0

    @property
    def has_paused_tasks(self):
        return len(self.get_tasks_by_status([statuses.PAUSED, statuses.PENDING])) > 0

    @property
    def has_canceling_tasks(self):
        return len(self.get_tasks_by_status([statuses.CANCELING])) > 0

    @property
    def has_canceled_tasks(self):
        return len(self.get_tasks_by_status([statuses.CANCELED])) > 0

    def get_unreachable_barriers(self):
        unreachable_barriers = []

        # Identify the list of barriers (or join tasks) in the workflow.
        barriers = self.conductor.graph.get_barriers()

        # Evaluate each task that is already staged.
        for staged_task in self.get_staged_tasks(filtered=False):
            # If the task is not a barrier or it is already ready, then it is not unreachable.
            if staged_task["id"] not in barriers or bool(staged_task["ready"]):
                continue

            # Determine the status of the inbound criteria for the barrier task.
            inbound_criteria_status = self.conductor.get_inbound_criteria_status(
                staged_task["id"], staged_task["route"]
            )

            # If the inbound criteria is not satisfied, then flag the task.
            if inbound_criteria_status == constants.INBOUND_CRITERIA_NOT_SATISFIED:
                unreachable_barriers.append(staged_task)

        return unreachable_barriers

    def get_staged_tasks(self, filtered=True):
        if not filtered:
            return self.staged

        return [x for x in self.staged if x["ready"] and not x.get("completed", False)]

    @property
    def has_staged_tasks(self):
        return len(self.get_staged_tasks()) > 0

    def add_staged_task(self, task_id, route, ctxs=None, prev=None, ready=True, retry=False):
        if not ctxs:
            ctxs = [0]

        entry = {
            "id": task_id,
            "ctxs": {"in": ctxs},
            "route": route,
            "prev": prev if isinstance(prev, dict) else {},
            "ready": ready,
        }

        if retry:
            entry["retry"] = retry

        self.staged.append(entry)

        return entry

    def get_staged_task(self, task_id, route):
        staged_tasks = [x for x in self.staged if x["id"] == task_id and x["route"] == route]

        return staged_tasks[0] if staged_tasks else None

    def remove_staged_task(self, task_id, route):
        staged_task = self.get_staged_task(task_id, route)

        if staged_task:
            any_items_running = [
                item
                for item in staged_task.get("items", [])
                if item["status"] in statuses.ACTIVE_STATUSES
            ]

            if not any_items_running:
                self.staged.remove(staged_task)


class WorkflowConductor(object):
    def __init__(self, spec, context=None, inputs=None):
        if not spec or not isinstance(spec, spec_base.Spec):
            raise ValueError('The value of "spec" is not type of Spec.')

        self.spec = spec
        self.catalog = self.spec.get_catalog()
        self.spec_module = spec_loader.get_spec_module(self.catalog)
        self.composer = plugin_util.get_module("orquesta.composers", self.catalog)

        self._errors = []
        self._graph = None
        self._inputs = inputs or {}
        self._log = []
        self._outputs = None
        self._parent_ctx = context or {}
        self._workflow_state = None

    def restore(
        self, graph, log=None, errors=None, state=None, inputs=None, outputs=None, context=None
    ):
        if not graph or not isinstance(graph, graphing.WorkflowGraph):
            raise ValueError('The value of "graph" is not type of WorkflowGraph.')

        if not state or not isinstance(state, WorkflowState):
            raise ValueError('The value of "state" is not type of WorkflowState.')

        if inputs is not None and not isinstance(inputs, dict):
            raise ValueError('The value of "inputs" is not type of dict.')

        if outputs is not None and not isinstance(outputs, dict):
            raise ValueError('The value of "outputs" is not type of dict.')

        self._errors = errors or []
        self._graph = graph
        self._inputs = inputs or {}
        self._log = log or []
        self._outputs = outputs
        self._parent_ctx = context or {}
        self._workflow_state = state

        # Assign a back reference of the conductor to the workflow state.
        # This back reference is needed to help the workflow state machine
        # identify if there are next tasks.
        self._workflow_state.conductor = self

    def serialize(self):
        return {
            "spec": self.spec.serialize(),
            "graph": self.graph.serialize(),
            "input": self.get_workflow_input(),
            "context": self.get_workflow_parent_context(),
            "state": self.workflow_state.serialize(),
            "log": json_util.deepcopy(self.log),
            "errors": json_util.deepcopy(self.errors),
            "output": self.get_workflow_output(),
        }

    @classmethod
    def deserialize(cls, data):
        spec_module = spec_loader.get_spec_module(data["spec"]["catalog"])
        spec = spec_module.WorkflowSpec.deserialize(data["spec"])

        graph = graphing.WorkflowGraph.deserialize(data["graph"])
        inputs = json_util.deepcopy(data["input"])
        context = json_util.deepcopy(data["context"])
        state = WorkflowState.deserialize(data["state"])
        log = json_util.deepcopy(data.get("log", []))
        errors = json_util.deepcopy(data["errors"])
        outputs = json_util.deepcopy(data["output"])

        instance = cls(spec)
        instance.restore(graph, log, errors, state, inputs, outputs, context)

        return instance

    @property
    def graph(self):
        if not self._graph:
            self._graph = self.composer.compose(self.spec)

        return self._graph

    @property
    def workflow_state(self):
        if not self._workflow_state:
            self._workflow_state = WorkflowState(conductor=self)

            # Set any given context as the initial context.
            init_ctx = self.get_workflow_parent_context()

            # Render workflow inputs and merge into the initial context.
            workflow_input = self.get_workflow_input()
            rendered_inputs, input_errors = self.spec.render_input(workflow_input, init_ctx)
            init_ctx = dict_util.merge_dicts(init_ctx, rendered_inputs, True)

            # Render workflow variables and merge into the initial context.
            rendered_vars, var_errors = self.spec.render_vars(init_ctx)
            init_ctx = dict_util.merge_dicts(init_ctx, rendered_vars, True)

            # Fail workflow if there are errors.
            errors = input_errors + var_errors

            if errors:
                self.log_errors(errors)
                self.request_workflow_status(statuses.FAILED)

            # Proceed if there is no issue with rendering of inputs and vars.
            if self.get_workflow_status() not in statuses.ABENDED_STATUSES:
                # Set the initial workflow context.
                self._workflow_state.contexts.append(init_ctx)

                # Set the initial execution route.
                self._workflow_state.routes.append([])

                # Identify the starting tasks and set the pointer to the initial context entry.
                for task_node in self.graph.roots:
                    ctxs, route = [0], 0
                    self._workflow_state.add_staged_task(
                        task_node["id"], route, ctxs=ctxs, ready=True
                    )

        return self._workflow_state

    @property
    def errors(self):
        return self._errors

    @property
    def log(self):
        return self._log

    def log_entry(
        self,
        entry_type,
        message,
        task_id=None,
        route=None,
        task_transition_id=None,
        result=None,
        data=None,
    ):

        # Check entry type.
        if entry_type not in ["info", "warn", "error"]:
            raise exc.WorkflowLogEntryError('The log entry type "%s" is not valid.' % entry_type)

        # Identify the appropriate log and then log the entry.
        log = self.errors if entry_type == "error" else self.log

        # Create the log entry.
        entry = {"type": entry_type, "message": message}
        dict_util.set_dict_value(entry, "task_id", task_id, insert_null=False)
        dict_util.set_dict_value(entry, "route", route, insert_null=False)
        dict_util.set_dict_value(entry, "task_transition_id", task_transition_id, insert_null=False)
        dict_util.set_dict_value(entry, "result", result, insert_null=False)
        dict_util.set_dict_value(entry, "data", data, insert_null=False)

        # Ignore if this is a duplicate.
        if len(list(filter(lambda x: x == entry, log))) > 0:
            return

        # Append the log entry.
        log.append(entry)

    def log_error(self, e, task_id=None, route=None, task_transition_id=None):
        self.log_entry(
            "error",
            "%s: %s" % (type(e).__name__, str(e)),
            task_id=task_id,
            route=route,
            task_transition_id=task_transition_id,
        )

    def log_errors(self, errors, task_id=None, route=None, task_transition_id=None):
        for error in errors:
            self.log_error(
                error, task_id=task_id, route=route, task_transition_id=task_transition_id
            )

    def get_workflow_parent_context(self):
        return json_util.deepcopy(self._parent_ctx)

    def get_workflow_input(self):
        return json_util.deepcopy(self._inputs)

    def get_workflow_status(self):
        return self.workflow_state.status

    def _set_workflow_status(self, value):
        if not machines.WorkflowStateMachine.is_transition_valid(self.workflow_state.status, value):
            raise exc.InvalidStatusTransition(self.workflow_state.status, value)

        self.workflow_state.status = value

    def request_workflow_status(self, status):
        # Record current workflow status.
        current_status = self.get_workflow_status()

        # Create an event for the request.
        wf_ex_event = events.WorkflowExecutionEvent(status)

        # Push the event to all the active tasks. The event may trigger status changes to the task.
        for idx, task_state in self.workflow_state.get_tasks_by_status(statuses.ACTIVE_STATUSES):
            machines.TaskStateMachine.process_event(self.workflow_state, task_state, wf_ex_event)

        # Process the workflow status change event.
        machines.WorkflowStateMachine.process_event(self.workflow_state, wf_ex_event)

        # Get workflow status after event is processed.
        updated_status = self.get_workflow_status()

        # Ignore if workflow hasn't changed from paused to pausing.
        if (
            status == statuses.PAUSED
            and current_status == statuses.PAUSING
            and updated_status == statuses.PAUSING
        ):
            return

        # Ignore if workflow hasn't changed from canceled to canceling.
        if (
            status == statuses.CANCELED
            and current_status == statuses.CANCELING
            and updated_status == statuses.CANCELING
        ):
            return

        # Otherwise, if status has not changed as expected, then raise exception.
        if status != current_status and current_status == updated_status:
            raise exc.InvalidWorkflowStatusTransition(current_status, wf_ex_event.name)

    def get_workflow_initial_context(self):
        return json_util.deepcopy(self.workflow_state.contexts[0])

    def get_workflow_terminal_context(self):
        if self.get_workflow_status() not in statuses.COMPLETED_STATUSES:
            raise exc.WorkflowContextError("Workflow is not in completed status.")

        wf_term_ctx = {}

        term_tasks = self.workflow_state.get_terminal_tasks()

        if not term_tasks:
            return wf_term_ctx

        _, first_term_task = term_tasks[0:1][0]
        other_term_tasks = term_tasks[1:]

        wf_term_ctx = self.get_task_context(first_term_task["ctxs"]["in"])

        for idx, task in other_term_tasks:
            # Remove the initial context since the first task processed above already
            # inclulded that and we only want to apply the differences.
            in_ctx_idxs = json_util.deepcopy(task["ctxs"]["in"])
            in_ctx_idxs.remove(0)

            wf_term_ctx = dict_util.merge_dicts(
                wf_term_ctx, self.get_task_context(in_ctx_idxs), overwrite=True
            )

        return wf_term_ctx

    def render_workflow_output(self):
        wf_status = self.get_workflow_status()

        # Render workflow outputs if workflow is completed.
        if wf_status in statuses.COMPLETED_STATUSES and not self._outputs:
            workflow_ctx = self.get_workflow_terminal_context()
            state_ctx = {"__state": self.workflow_state.serialize()}
            workflow_ctx = dict_util.merge_dicts(workflow_ctx, state_ctx, True)
            outputs, errors = self.spec.render_output(workflow_ctx)

            # Persist outputs if it is not empty.
            if outputs:
                self._outputs = outputs

            # Log errors if any returned and mark workflow as failed.
            if errors:
                self.log_errors(errors)

                if wf_status not in [statuses.EXPIRED, statuses.ABANDONED, statuses.CANCELED]:
                    self.request_workflow_status(statuses.FAILED)

    def get_workflow_output(self):
        return json_util.deepcopy(self._outputs) if self._outputs else None

    def reset_workflow_output(self):
        self._outputs = None

    def get_inbound_criteria_status(self, task_id, route):
        # Get the list of inbound task transitions for the barrier task.
        inbound_transitions = self.graph.get_prev_transitions(task_id)

        # Setup the result for the evaluation of the criteria for inbound task transitions.
        inbound_evaluation = {i: None for i in list(set(t[0] for t in inbound_transitions))}

        # Identify the join requirement.
        barrier = self.graph.get_barrier(task_id) or 1
        requirement = len(inbound_evaluation.keys()) if barrier == "*" else barrier

        # Evaluate the criteria for each inbound task transitions.
        for prev_transition in inbound_transitions:
            prev_task_state_entry = self.get_task_state_entry(prev_transition[0], route)

            if not prev_task_state_entry:
                continue

            prev_task_transition_id = constants.TASK_STATE_TRANSITION_FORMAT % (
                prev_transition[1],
                str(prev_transition[2]),
            )

            satisfied = (
                prev_task_transition_id in prev_task_state_entry["next"]
                and prev_task_state_entry["next"][prev_task_transition_id]
            )

            if not bool(inbound_evaluation[prev_transition[0]]):
                inbound_evaluation[prev_transition[0]] = satisfied

        # If the count of inbound task(s) where the criteria is True >= requirements,
        # then the join requirement is satisified.
        if list(inbound_evaluation.values()).count(True) >= requirement:
            return constants.INBOUND_CRITERIA_SATISFIED

        # If there is an inbound task(s) where the criteria is None and there is still
        # active task(s) or staged task(s) that is ready,  then this means that the
        # workflow is still active and it is possible that not all inbound branch(es)
        # and subsequent task(s) have run.
        if None in inbound_evaluation.values() and (
            self.workflow_state.has_active_tasks or self.workflow_state.has_staged_tasks
        ):
            return constants.INBOUND_CRITERIA_WIP

        # If reached here, then the requirement is not satisified.
        return constants.INBOUND_CRITERIA_NOT_SATISFIED

    def get_task(self, task_id, route):
        try:
            task_ctx = self.get_task_initial_context(task_id, route)
        except ValueError:
            task_ctx = self.get_workflow_initial_context()

        state_ctx = {"__state": self.workflow_state.serialize()}
        current_task = {"id": task_id, "route": route}
        task_ctx = ctx_util.set_current_task(task_ctx, current_task)
        task_ctx = dict_util.merge_dicts(task_ctx, state_ctx, True)
        task_spec = self.spec.tasks.get_task(task_id).copy()
        task_spec, action_specs = task_spec.render(task_ctx)

        task = {
            "id": task_id,
            "route": route,
            "ctx": task_ctx,
            "spec": task_spec,
            "actions": action_specs,
        }

        # If there is a task delay specified, evaluate the delay value.
        if getattr(task_spec, "delay", None):
            task_delay = task_spec.delay

            if isinstance(task_delay, six.string_types):
                task_delay = expr_base.evaluate(task_delay, task_ctx)

            if not isinstance(task_delay, int):
                raise TypeError("The value of task delay is not type of integer.")

            task["delay"] = task_delay

        # Add items and related meta data to the task details.
        if task_spec.has_items():
            items_spec = getattr(task_spec, "with")
            concurrency = getattr(items_spec, "concurrency", None)
            task["items_count"] = len(action_specs)
            task["concurrency"] = expr_base.evaluate(concurrency, task_ctx)

        return task

    def _evaluate_task_actions(self, task):
        task_id = task["id"]
        task_route = task["route"]

        # Return task if it is not with items.
        if not task["spec"].has_items():
            return task

        # Fetch the task entry from staging.
        staged_task = self.workflow_state.get_staged_task(task_id, task_route)

        # Prepare the staging task to track items execution status.
        if "items" not in staged_task or not staged_task["items"]:
            staged_task["items"] = [{"status": statuses.UNSET}] * task["items_count"]

        # Trim the list of actions in the task per concurrency policy.
        all_items = list(zip(task["actions"], staged_task["items"]))
        notrun_items = list(filter(lambda x: x[1]["status"] == statuses.UNSET, all_items))
        active_items = list(filter(lambda x: x[1]["status"] in statuses.ACTIVE_STATUSES, all_items))

        if task["concurrency"] is not None:
            availability = task["concurrency"] - len(active_items)
            candidates = list(zip(*notrun_items[:availability]))
            task["actions"] = list(candidates[0]) if candidates and availability > 0 else []
        else:
            candidates = list(zip(*notrun_items))
            task["actions"] = list(candidates[0]) if candidates else []

        return task

    def has_next_tasks(self, task_id=None, route=None):
        if not task_id:
            return True if self.workflow_state.get_staged_tasks() else False
        else:
            task_state_entry = self.get_task_state_entry(task_id, route)

            if (
                not task_state_entry
                or task_state_entry.get("status") not in statuses.COMPLETED_STATUSES
            ):
                return []

            outbounds = self.graph.get_next_transitions(task_id)

            for next_seq in outbounds:
                next_task_id, seq_key = next_seq[1], next_seq[2]
                task_transition_id = constants.TASK_STATE_TRANSITION_FORMAT % (
                    next_task_id,
                    str(seq_key),
                )

                # Ignore if the next task is the engine command to "continue".
                if next_task_id == "continue":
                    continue

                # Evaluate if outbound criteria is satisfied.
                if not task_state_entry["next"].get(task_transition_id):
                    continue

                # Evaluate if inbound criteria is satisified for barrier (join) task.
                if self.graph.has_barrier(next_task_id):
                    inbound_criteria_status = self.get_inbound_criteria_status(next_task_id, route)
                    if inbound_criteria_status == constants.INBOUND_CRITERIA_NOT_SATISFIED:
                        continue

                return True

        return False

    def get_next_tasks(self):
        fail_on_task_rendering = False
        staged_tasks = self.workflow_state.get_staged_tasks()
        remediation_tasks = []
        next_tasks = []

        # Identify remediation tasks if workflow failed.
        if self.get_workflow_status() == statuses.FAILED:
            remediation_tasks = [s for s in staged_tasks if s.get("run_on_fail", False) is True]

        # Return an empty list if the workflow is not running and there is no remediation tasks.
        if self.get_workflow_status() not in statuses.RUNNING_STATUSES and not remediation_tasks:
            return next_tasks

        # Return the list of tasks that are staged and readied. If there is exception on
        # task rendering, then log the error and continue. This allows user to know about
        # all task rendering errors for this task transition instead of getting rendering
        # error one at a time during runtime.
        for staged_task in remediation_tasks or staged_tasks:
            try:
                next_task = self.get_task(staged_task["id"], staged_task["route"])
                next_task = self._evaluate_task_actions(next_task)

                # Assign the task retry delay which will overwrite any task delay
                # specified in the task definition.
                if "retry" in staged_task:
                    next_task["delay"] = staged_task["retry"].get("delay") or 0

                if "actions" in next_task and len(next_task["actions"]) > 0:
                    next_tasks.append(next_task)
                elif "items_count" in next_task and next_task["items_count"] == 0:
                    next_tasks.append(next_task)
            except Exception as e:
                fail_on_task_rendering = True
                self.log_error(e, task_id=staged_task["id"], route=staged_task["route"])
                continue

        # Return nothing if there is error(s) on determining next tasks.
        if fail_on_task_rendering:
            self.request_workflow_status(statuses.FAILED)
            return []

        return sorted(next_tasks, key=lambda x: (x["id"], x["route"]))

    def _get_task_state_idx(self, task_id, route):
        return self.workflow_state.tasks.get(
            constants.TASK_STATE_ROUTE_FORMAT % (task_id, str(route))
        )

    def get_task_state_entry(self, task_id, route):
        task_state_seq_idx = self._get_task_state_idx(task_id, route)

        if task_state_seq_idx is None:
            return None

        return self.workflow_state.sequence[task_state_seq_idx]

    def make_task_context(self, task_state_entry, task_result=None):
        in_ctx_idxs = task_state_entry["ctxs"]["in"]
        in_ctx_val = self.get_task_context(in_ctx_idxs)

        current_task = {
            "id": task_state_entry["id"],
            "route": task_state_entry["route"],
            "result": task_result,
        }

        current_ctx = ctx_util.set_current_task(in_ctx_val, current_task)
        state_ctx = {"__state": self.workflow_state.serialize()}
        current_ctx = dict_util.merge_dicts(current_ctx, state_ctx, True)

        return current_ctx

    def make_task_result(self, task_spec, event):
        # Format task result depending on the type of task.
        if not task_spec.has_items():
            task_result = event.result
        else:
            # For with items task, use the accumulated result from the event.
            task_result = (
                event.accumulated_result or []
                if isinstance(event, events.TaskItemActionExecutionEvent)
                else event.result or []
            )

        return task_result

    def setup_retry_in_task_state(self, task_state_entry, in_ctx_idxs):
        # Setup the retry in the task state.
        task_id = task_state_entry["id"]
        task_retry_spec = self.graph.get_task_retry_spec(task_id)
        task_state_entry["retry"] = json_util.deepcopy(task_retry_spec)
        task_state_entry["retry"]["tally"] = 0

        # Get task context for evaluating the expression in delay and count.
        in_ctx = self.get_task_context(in_ctx_idxs)

        # Evaluate the retry delay value.
        if "delay" in task_state_entry["retry"] and isinstance(
            task_state_entry["retry"]["delay"], six.string_types
        ):
            delay_value = expr_base.evaluate(task_state_entry["retry"]["delay"], in_ctx)

            if not isinstance(delay_value, int):
                raise ValueError('The retry delay for task "%s" is not an integer.' % task_id)

            task_state_entry["retry"]["delay"] = delay_value

        # Evaluate the retry count value.
        if "count" in task_state_entry["retry"] and isinstance(
            task_state_entry["retry"]["count"], six.string_types
        ):
            count_value = expr_base.evaluate(task_state_entry["retry"]["count"], in_ctx)

            if not isinstance(count_value, int):
                raise ValueError('The retry count for task "%s" is not an integer.' % task_id)

            task_state_entry["retry"]["count"] = count_value

    def add_task_state(self, task_id, route, in_ctx_idxs=None, prev=None):
        if not self.graph.has_task(task_id):
            raise exc.InvalidTask(task_id)

        if not in_ctx_idxs:
            in_ctx_idxs = [0]

        task_state_entry = {
            "id": task_id,
            "route": route,
            "ctxs": {"in": in_ctx_idxs},
            "prev": prev or {},
            "next": {},
        }

        # If the task has retry spec defined, then setup the retry in the task state entry.
        if self.graph.task_has_retry(task_id):
            self.setup_retry_in_task_state(task_state_entry, in_ctx_idxs)

        # Append the task state entry to the list of task execution.
        task_state_entry_id = constants.TASK_STATE_ROUTE_FORMAT % (task_id, str(route))
        self.workflow_state.sequence.append(task_state_entry)
        self.workflow_state.tasks[task_state_entry_id] = len(self.workflow_state.sequence) - 1

        return task_state_entry

    def update_task_state(self, task_id, route, event):
        engine_event_queue = queue.Queue()

        # Throw exception if not expected event type.
        if not issubclass(type(event), events.ExecutionEvent):
            raise TypeError("Event is not type of ExecutionEvent.")

        # Throw exception if task does not exist in the workflow graph.
        if not self.graph.has_task(task_id):
            raise exc.InvalidTask(task_id)

        # Try to get the task metadata from staging or task state.
        staged_task = self.workflow_state.get_staged_task(task_id, route)
        task_state_entry = self.get_task_state_entry(task_id, route)

        # Get the task spec for the task which contains additional meta data.
        task_spec = self.spec.tasks.get_task(task_id)

        # Throw exception if task is not staged and there is no task state entry.
        if not staged_task and not task_state_entry:
            raise exc.InvalidTaskStateEntry(task_id)

        # Create new task state entry if it does not exist or if it is an engine command.
        if not task_state_entry or task_id in events.ENGINE_EVENT_MAP.keys():
            task_state_entry = self.add_task_state(
                task_id,
                staged_task["route"],
                in_ctx_idxs=staged_task["ctxs"]["in"],
                prev=staged_task["prev"],
            )

        # Identify the index for the task state object for later use.
        task_state_idx = self._get_task_state_idx(task_id, route)

        # If task is already completed and in cycle, then create new task state entry.
        # Unfortunately, the method in the graph to check for cycle is too simple and
        # misses forks that extends from the cycle. The check here assumes that the
        # last task entry is already completed and the new task status is one of the
        # starting statuses, then there is high likelihood that this is a cycle.
        if (
            task_state_entry.get("status") in statuses.COMPLETED_STATUSES
            and event.status
            and event.status in statuses.STARTING_STATUSES
        ):
            task_state_entry = self.add_task_state(
                task_id,
                staged_task["route"],
                in_ctx_idxs=staged_task["ctxs"]["in"],
                prev=staged_task["prev"],
            )

            # Update the index value since a new entry is created.
            task_state_idx = self._get_task_state_idx(task_id, route)

        # Remove task from staging if task is not with items.
        if event.status and staged_task and "items" not in staged_task:
            self.workflow_state.remove_staged_task(task_id, route)

        # If action execution is for a task item, then record the execution status for the item.
        # Result for each item is not recorded in the staged_task because it impacts database
        # write performance if there are a lot of items and/or item result size is huge.
        if staged_task and isinstance(event, events.TaskItemActionExecutionEvent):
            staged_task["items"][event.item_id] = {"status": event.status}

        # Log the error if it is a failed execution event.
        if event.status == statuses.FAILED:
            message = "Execution failed. See result for details."
            self.log_entry("error", message, task_id=task_id, result=event.result)

        # Process the action execution event using the
        # task state machine and update the task status.
        old_task_status = task_state_entry.get("status", statuses.UNSET)
        machines.TaskStateMachine.process_event(self.workflow_state, task_state_entry, event)
        new_task_status = task_state_entry.get("status", statuses.UNSET)

        # If retrying, staged the task to be returned in get_next_tasks.
        if new_task_status == statuses.RETRYING:
            # Increment the number of times that the task has retried.
            task_state_entry["retry"]["tally"] += 1

            # Reset the staged task to be returned in get_next_tasks
            self.workflow_state.remove_staged_task(task_id, route)

            self.workflow_state.add_staged_task(
                task_id,
                route,
                ctxs=task_state_entry["ctxs"]["in"],
                prev=task_state_entry["prev"],
                retry=task_state_entry["retry"],
                ready=True,
            )

        # Get task result and set current context if task is completed.
        if new_task_status in statuses.COMPLETED_STATUSES:
            # Remove task from staging if exists but keep and flag entry
            # if task has items and failed for manual rerun.
            if not (task_spec.has_items() and new_task_status in statuses.ABENDED_STATUSES):
                self.workflow_state.remove_staged_task(task_id, route)
            else:
                staged_task = self.workflow_state.get_staged_task(task_id, route)
                staged_task["completed"] = True

            # Format task result depending on the type of task.
            task_result = self.make_task_result(task_spec, event)

            # Set current task in the context.
            current_ctx = self.make_task_context(task_state_entry, task_result=task_result)

            # Evaluate if there is a task retry. The task retry can only be evaluated after
            # the state machine has determined the status for the task execution. If the task
            # is completed, get the task result and context which is required to evaluate the
            # the condition if a retry for the task is required.
            if self.get_workflow_status() in statuses.ACTIVE_STATUSES and self._evaluate_task_retry(
                task_state_entry, current_ctx
            ):
                return self.update_task_state(task_id, route, events.TaskRetryEvent())

        # Evaluate task transitions if task is completed and status change is not processed.
        if new_task_status in statuses.COMPLETED_STATUSES and new_task_status != old_task_status:
            has_manual_fail = False
            staged_next_tasks = []

            # Identify task transitions for the current completed task.
            task_transitions = self.graph.get_next_transitions(task_id)

            # Mark task as terminal when there is no transitions.
            if not task_transitions:
                task_state_entry["term"] = True

            # Iterate thru each outbound task transitions.
            for task_transition in task_transitions:
                task_transition_id = constants.TASK_STATE_TRANSITION_FORMAT % (
                    task_transition[1],
                    str(task_transition[2]),
                )

                # Evaluate the criteria for task transition. If there is a failure while
                # evaluating expression(s), fail the workflow.
                try:
                    criteria = task_transition[3].get("criteria") or []
                    evaluated_criteria = [expr_base.evaluate(c, current_ctx) for c in criteria]
                    task_state_entry["next"][task_transition_id] = all(evaluated_criteria)
                except Exception as e:
                    self.log_error(e, task_id, route, task_transition_id)
                    self.request_workflow_status(statuses.FAILED)
                    continue

                # If criteria met, then mark the next task staged and calculate outgoing context.
                if task_state_entry["next"][task_transition_id]:
                    next_task_node = self.graph.get_task(task_transition[1])
                    next_task_id = next_task_node["id"]
                    new_ctx_idx = None

                    # Get and process new context for the task transition.
                    out_ctx, new_ctx, errors = task_spec.finalize_context(
                        next_task_id, task_transition, json_util.deepcopy(current_ctx)
                    )

                    if errors:
                        self.log_errors(errors, task_id, route, task_transition_id)
                        self.request_workflow_status(statuses.FAILED)
                        continue

                    out_ctx_idxs = json_util.deepcopy(task_state_entry["ctxs"]["in"])

                    if new_ctx:
                        self.workflow_state.contexts.append(new_ctx)
                        new_ctx_idx = len(self.workflow_state.contexts) - 1

                        # Add to the list of contexts for the next task in this transition.
                        out_ctx_idxs.append(new_ctx_idx)

                        # Record the outgoing context for this task transition.
                        if "out" not in task_state_entry["ctxs"]:
                            task_state_entry["ctxs"]["out"] = {}

                        task_state_entry["ctxs"]["out"] = {task_transition_id: new_ctx_idx}

                    # Stage the next task if it is not in staging.
                    next_task_route = self._evaluate_route(task_transition, route)

                    staged_next_task = self.workflow_state.get_staged_task(
                        next_task_id, next_task_route
                    )

                    backref = constants.TASK_STATE_TRANSITION_FORMAT % (
                        task_id,
                        str(task_transition[2]),
                    )

                    # If the next task is already staged.
                    if staged_next_task:
                        # Remove the root context to avoid overwriting vars.
                        out_ctx_idxs.remove(0)

                        # Extend the outgoing context from this task.
                        staged_next_task["ctxs"]["in"].extend(out_ctx_idxs)

                        # Add a backref for the current task in the next task.
                        staged_next_task["prev"][backref] = task_state_idx

                        # Clear list of items for with items task.
                        staged_next_task.pop("items", None)
                        staged_next_task.pop("completed", None)
                    else:
                        # Otherwise create a new entry in staging for the next task.
                        staged_next_task = self.workflow_state.add_staged_task(
                            next_task_id,
                            next_task_route,
                            ctxs=out_ctx_idxs,
                            prev={backref: task_state_idx},
                            ready=False,
                        )

                    # Check if inbound criteria are met. Must use the original route
                    # to identify the inbound task transitions.
                    staged_next_task["ready"] = (
                        self.get_inbound_criteria_status(next_task_id, route)
                        == constants.INBOUND_CRITERIA_SATISFIED
                    )

                    # Put the next task in the engine event queue if it is an engine command.
                    if next_task_id in events.ENGINE_EVENT_MAP.keys():
                        queue_entry = (staged_next_task["id"], staged_next_task["route"])
                        engine_event_queue.put(queue_entry)

                        # Flag if there is at least one fail command in the task transition.
                        if not has_manual_fail:
                            has_manual_fail = next_task_id == "fail"
                    else:
                        # If not an engine command and the next task is ready, then
                        # make a record of it for processing manual fail below.
                        if staged_next_task["ready"]:
                            staged_next_tasks.append(staged_next_task)

            # Task failure is remediable. For example, there may be workflow that wants
            # to run a cleanup task on failure. In certain cases, we still want to fail
            # the workflow after the remediation. The fail command can be in the
            # task transition under the cleanup task. However, the cleanup task may be
            # reusable when either workflow succeed or fail. It does not make sense to
            # put the fail command in the task transition of the cleanup task. So we
            # want to allow user to be able to define both the cleanup and fail in the
            # task transition under the current task but still return the cleanup task
            # even when the workflow is set to failed status.
            if has_manual_fail:
                for staged_next_task in staged_next_tasks:
                    staged_next_task["run_on_fail"] = True

        # Process the task event using the workflow state machine and update the workflow status.
        task_ex_event = events.TaskExecutionEvent(task_id, route, task_state_entry["status"])
        machines.WorkflowStateMachine.process_event(self.workflow_state, task_ex_event)

        # Process any engine commands in the queue.
        while not engine_event_queue.empty():
            next_task_id, next_task_route = engine_event_queue.get()
            engine_event = events.ENGINE_EVENT_MAP[next_task_id]
            self.update_task_state(next_task_id, next_task_route, engine_event())

        # Mark the task as a terminal task if workflow execution is completed.
        if self.get_workflow_status() in statuses.COMPLETED_STATUSES:
            task_state_entry["term"] = True

        return task_state_entry

    def _evaluate_route(self, task_transition, prev_route):
        task_id = task_transition[1]

        prev_task_transition_id = constants.TASK_STATE_TRANSITION_FORMAT % (
            task_transition[0],
            str(task_transition[2]),
        )

        is_split_task = self.spec.tasks.is_split_task(task_id)
        is_in_cycle = self.graph.in_cycle(task_id)

        if not is_split_task or is_in_cycle:
            return prev_route

        old_route_details = self.workflow_state.routes[prev_route]
        new_route_details = json_util.deepcopy(old_route_details)

        if prev_task_transition_id not in old_route_details:
            new_route_details.append(prev_task_transition_id)

        if old_route_details == new_route_details:
            return prev_route

        self.workflow_state.routes.append(new_route_details)

        return len(self.workflow_state.routes) - 1

    def _evaluate_task_retry(self, task_state_entry, current_ctx):
        if "retry" not in task_state_entry:
            return False

        task_status = task_state_entry.get("status", statuses.UNSET)
        retry_count = task_state_entry["retry"]["count"]
        retry_tally = task_state_entry["retry"]["tally"]

        if retry_tally >= retry_count:
            return False

        if task_status in statuses.ABENDED_STATUSES and task_state_entry["retry"]["when"] is None:
            return True

        if expr_base.evaluate(task_state_entry["retry"]["when"], current_ctx):
            return True

        return False

    def get_task_context(self, ctx_idxs):
        ctx = {}

        for ctx_idx in ctx_idxs:
            ctx = dict_util.merge_dicts(ctx, self.workflow_state.contexts[ctx_idx], overwrite=True)

        return ctx

    def get_task_initial_context(self, task_id, route):
        staged_task = self.workflow_state.get_staged_task(task_id, route)

        if staged_task:
            return self.get_task_context(staged_task["ctxs"]["in"])

        task_state_entry = self.get_task_state_entry(task_id, route)

        if task_state_entry:
            return self.get_task_context(task_state_entry["ctxs"]["in"])

        raise ValueError('Unable to determine context for task "%s".' % task_id)

    def get_task_transition_contexts(self, task_id, route):
        contexts = {}

        task_state_entry = self.get_task_state_entry(task_id, route)

        if not task_state_entry:
            raise exc.InvalidTaskStateEntry(task_id)

        for t in self.graph.get_next_transitions(task_id):
            task_transition_id = constants.TASK_STATE_TRANSITION_FORMAT % (t[1], str(t[2]))

            if (
                task_transition_id in task_state_entry["next"]
                and task_state_entry["next"][task_transition_id]
            ):
                contexts[task_transition_id] = self.get_task_initial_context(t[1], route)

        return contexts

    def _request_task_rerun(self, task_id, route, reset_items=False):
        task = self.workflow_state.get_task(task_id, route)
        task_ctx = json_util.deepcopy(task["ctxs"]["in"])
        task_prev = json_util.deepcopy(task["prev"])
        task_spec = self.spec.tasks.get_task(task_id)

        # Reset terminal status for the rerunnable candidate.
        task.pop("term", None)
        task.pop("ignore", None)

        # Reset staged task for the rerunnable candidate.
        staged_task = self.workflow_state.get_staged_task(task_id, route)

        if staged_task:
            staged_task.pop("completed", None)

        # Reset the list of errors for the task.
        for e in [e for e in self.errors if e.get("task_id", None) == task_id]:
            self.errors.remove(e)

        # If task has items, then use existing staged task entry and reset failed items.
        if task_spec.has_items():
            staged_task = self.workflow_state.get_staged_task(task_id, route)
            for item in staged_task.get("items", []):
                if reset_items or item["status"] in statuses.ABENDED_STATUSES:
                    item["status"] = statuses.UNSET
        # Otherwise, add a new task state entry and stage task to be returned in get_next_tasks.
        else:
            self.add_task_state(task_id, route, in_ctx_idxs=task_ctx, prev=task_prev)
            self.workflow_state.add_staged_task(task_id, route, ctxs=task_ctx, prev=task_prev)

        # Reset terminal status for the task branch which will also be rerun.
        for _, next_task in self.workflow_state.get_task_sequence(task_id, route):
            next_task.pop("term", None)

    def _collapse_task_rerun_requests(self, tasks=None):
        # Get the subsequent sequence of tasks that already ran for each task in the task requests.
        # The method get_task_sequence returns the index and the dictionary for the task entry.
        # Only the index is required for further evaluation below.
        result = {
            k: [i[0] for i in self.workflow_state.get_task_sequence(t.task_id, t.route)]
            for k, t in six.iteritems(tasks)
        }

        # If the list of task request is greater than one, then we have to check whether
        # there are task rerun requests that are subsequent task executions for another
        # task rerun requests. If they are not collapsed/consolidated, then the rerun
        # will result in multiple branches of executions.
        if len(tasks) > 1:
            # The for loops below identify task requests that have subsequent task sequences
            # not in other task requests.
            result = {
                k: i
                for k, i in six.iteritems(result)
                for j in result.values()
                if len(set(i) - set(j)) > 0
            }

        return result

    def request_workflow_rerun(self, task_requests=None):
        # Throw exception if workflow is still active.
        if self.get_workflow_status() not in statuses.COMPLETED_STATUSES:
            raise exc.WorkflowIsActiveAndNotRerunableError()

        # Concatenate task id and route so it is easier to use in filter below.
        # This conversion also removes any duplicate task rerun requests.
        tasks = {t.task_state_entry_id: t for t in task_requests or []}

        # If the list of tasks is provided, verify if task exist and rerunnable.
        invalid_rerun_requests = [
            t for k, t in six.iteritems(tasks) if k not in self.workflow_state.tasks
        ]

        if invalid_rerun_requests:
            raise exc.InvalidTaskRerunRequest(invalid_rerun_requests)

        # If no tasks specified, then get the list of terminal tasks that abended.
        if not tasks:
            rerunnable_candidates = {
                constants.TASK_STATE_ROUTE_FORMAT % (t["id"], str(t["route"])): (i, t)
                for i, t in self.workflow_state.get_terminal_tasks()
                if "status" in t and t["status"] in statuses.ABENDED_STATUSES
            }
        # Otherwise if the list of tasks is provided, then filter the list of rerun candidates.
        else:
            rerunnable_candidates = {
                k: (
                    self._get_task_state_idx(t.task_id, t.route),
                    self.workflow_state.get_task(t.task_id, t.route),
                )
                for k, t in six.iteritems(tasks)
                if k in self._collapse_task_rerun_requests(tasks)
            }

        # Keep record of which task sequence(s) is being rerun in the workflow state.
        rerun_entry = [i for i, t in rerunnable_candidates.values()]
        self.workflow_state.reruns.append(rerun_entry)

        # Setup task candidates for rerun.
        sorted_rerunnable_candidates = sorted(
            rerunnable_candidates.values(), key=lambda x: (x[1]["id"], x[1]["route"])
        )

        for _, task in sorted_rerunnable_candidates:
            k = constants.TASK_STATE_ROUTE_FORMAT % (task["id"], str(task["route"]))
            reset_items = False if k not in tasks else tasks[k].reset_items
            self._request_task_rerun(task["id"], task["route"], reset_items=reset_items)

        # Get the list of terminal tasks with next or remediation task(s).
        continuable_candidates = {
            constants.TASK_STATE_ROUTE_FORMAT % (t["id"], str(t["route"])): t
            for i, t in self.workflow_state.get_terminal_tasks()
            if len([k for k, v in six.iteritems(t["next"]) if v]) > 0
        }

        # Automatically resume all continuable candidates.
        for _, task in sorted(six.iteritems(continuable_candidates), key=lambda x: x[0]):
            # Reset terminal status for the continuable candidate.
            task.pop("term", None)

        # Reset the workflow output.
        self.reset_workflow_output()

        # Finally, reset workflow status to resuming if preparation above succeeded.
        self.workflow_state.status = statuses.RESUMING
