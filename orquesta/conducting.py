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

import copy
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

    def serialize(self):
        return {
            'contexts': copy.deepcopy(self.contexts),
            'routes': copy.deepcopy(self.routes),
            'sequence': copy.deepcopy(self.sequence),
            'staged': copy.deepcopy(self.staged),
            'status': self.status,
            'tasks': copy.deepcopy(self.tasks)
        }

    @classmethod
    def deserialize(cls, data):
        instance = cls()
        instance.contexts = copy.deepcopy(data.get('contexts', list()))
        instance.routes = copy.deepcopy(data.get('routes', list()))
        instance.sequence = copy.deepcopy(data.get('sequence', list()))
        instance.staged = copy.deepcopy(data.get('staged', dict()))
        instance.status = data.get('status', statuses.UNSET)
        instance.tasks = copy.deepcopy(data.get('tasks', dict()))

        return instance

    def get_task(self, task_id, task_route):
        return self.sequence[
            self.tasks[constants.TASK_STATE_ROUTE_FORMAT % (task_id, str(task_route))]
        ]

    def get_tasks_by_status(self, statuses):
        return [t for t in self.sequence if t['status'] in statuses]

    def get_terminal_tasks(self):
        return [t for t in self.sequence if t.get('term', False)]

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

    def get_staged_tasks(self):
        return list(filter(lambda x: x['ready'] is True, self.staged))

    @property
    def has_staged_tasks(self):
        return len(self.get_staged_tasks()) > 0

    def add_staged_task(self, task_id, route, ctxs=None, prev=None, ready=True):
        if not ctxs:
            ctxs = [0]

        entry = {
            'id': task_id,
            'ctxs': {
                'in': ctxs
            },
            'route': route,
            'prev': prev if isinstance(prev, dict) else {},
            'ready': ready
        }

        self.staged.append(entry)

        return entry

    def get_staged_task(self, task_id, route):
        def query(x):
            return x['id'] == task_id and x['route'] == route

        staged_tasks = list(filter(query, self.staged))

        return staged_tasks[0] if staged_tasks else None

    def remove_staged_task(self, task_id, route):
        staged_task = self.get_staged_task(task_id, route)

        if staged_task:
            any_items_running = [
                item for item in staged_task.get('items', [])
                if item['status'] in statuses.ACTIVE_STATUSES
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
        self.composer = plugin_util.get_module('orquesta.composers', self.catalog)

        self._errors = []
        self._graph = None
        self._inputs = inputs or {}
        self._log = []
        self._outputs = None
        self._parent_ctx = context or {}
        self._workflow_state = None

    def restore(self, graph, log=None, errors=None, state=None,
                inputs=None, outputs=None, context=None):
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
            'spec': self.spec.serialize(),
            'graph': self.graph.serialize(),
            'input': self.get_workflow_input(),
            'context': self.get_workflow_parent_context(),
            'state': self.workflow_state.serialize(),
            'log': copy.deepcopy(self.log),
            'errors': copy.deepcopy(self.errors),
            'output': self.get_workflow_output()
        }

    @classmethod
    def deserialize(cls, data):
        spec_module = spec_loader.get_spec_module(data['spec']['catalog'])
        spec = spec_module.WorkflowSpec.deserialize(data['spec'])

        graph = graphing.WorkflowGraph.deserialize(data['graph'])
        inputs = copy.deepcopy(data['input'])
        context = copy.deepcopy(data['context'])
        state = WorkflowState.deserialize(data['state'])
        log = copy.deepcopy(data.get('log', []))
        errors = copy.deepcopy(data['errors'])
        outputs = copy.deepcopy(data['output'])

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
                        task_node['id'],
                        route,
                        ctxs=ctxs,
                        ready=True
                    )

        return self._workflow_state

    @property
    def errors(self):
        return self._errors

    @property
    def log(self):
        return self._log

    def log_entry(self, entry_type, message, task_id=None, route=None,
                  task_transition_id=None, result=None, data=None):

        # Check entry type.
        if entry_type not in ['info', 'warn', 'error']:
            raise exc.WorkflowLogEntryError('The log entry type "%s" is not valid.' % entry_type)

        # Identify the appropriate log and then log the entry.
        log = self.errors if entry_type == 'error' else self.log

        # Create the log entry.
        entry = {'type': entry_type, 'message': message}
        dict_util.set_dict_value(entry, 'task_id', task_id, insert_null=False)
        dict_util.set_dict_value(entry, 'route', route, insert_null=False)
        dict_util.set_dict_value(entry, 'task_transition_id', task_transition_id, insert_null=False)
        dict_util.set_dict_value(entry, 'result', result, insert_null=False)
        dict_util.set_dict_value(entry, 'data', data, insert_null=False)

        # Ignore if this is a duplicate.
        if len(list(filter(lambda x: x == entry, log))) > 0:
            return

        # Append the log entry.
        log.append(entry)

    def log_error(self, e, task_id=None, route=None, task_transition_id=None):
        self.log_entry(
            'error',
            '%s: %s' % (type(e).__name__, str(e)),
            task_id=task_id,
            route=route,
            task_transition_id=task_transition_id
        )

    def log_errors(self, errors, task_id=None, route=None, task_transition_id=None):
        for error in errors:
            self.log_error(
                error,
                task_id=task_id,
                route=route,
                task_transition_id=task_transition_id
            )

    def get_workflow_parent_context(self):
        return copy.deepcopy(self._parent_ctx)

    def get_workflow_input(self):
        return copy.deepcopy(self._inputs)

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
        for task_state in self.workflow_state.get_tasks_by_status(statuses.ACTIVE_STATUSES):
            machines.TaskStateMachine.process_event(self.workflow_state, task_state, wf_ex_event)

        # Process the workflow status change event.
        machines.WorkflowStateMachine.process_event(self.workflow_state, wf_ex_event)

        # Get workflow status after event is processed.
        updated_status = self.get_workflow_status()

        # If status has not changed as expected, then raise exception.
        if status != current_status and current_status == updated_status:
            raise exc.InvalidWorkflowStatusTransition(current_status, wf_ex_event.name)

    def get_workflow_initial_context(self):
        return copy.deepcopy(self.workflow_state.contexts[0])

    def get_workflow_terminal_context(self):
        if self.get_workflow_status() not in statuses.COMPLETED_STATUSES:
            raise exc.WorkflowContextError('Workflow is not in completed status.')

        wf_term_ctx = {}

        term_tasks = self.workflow_state.get_terminal_tasks()

        if not term_tasks:
            return wf_term_ctx

        first_term_task = term_tasks[0:1][0]
        other_term_tasks = term_tasks[1:]

        wf_term_ctx = self.get_task_context(first_term_task['ctxs']['in'])

        for task in other_term_tasks:
            # Remove the initial context since the first task processed above already
            # inclulded that and we only want to apply the differences.
            in_ctx_idxs = copy.deepcopy(task['ctxs']['in'])
            in_ctx_idxs.remove(0)

            wf_term_ctx = dict_util.merge_dicts(
                wf_term_ctx,
                self.get_task_context(in_ctx_idxs),
                overwrite=True
            )

        return wf_term_ctx

    def _render_workflow_outputs(self):
        wf_status = self.get_workflow_status()

        # Render workflow outputs if workflow is completed.
        if wf_status in statuses.COMPLETED_STATUSES and not self._outputs:
            workflow_ctx = self.get_workflow_terminal_context()
            state_ctx = {'__state': self.workflow_state.serialize()}
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
        return copy.deepcopy(self._outputs) if self._outputs else None

    def _inbound_criteria_satisfied(self, task_id, route):
        inbounds = self.graph.get_prev_transitions(task_id)
        inbounds_satisfied = []
        barrier = 1

        if self.graph.has_barrier(task_id):
            barrier = self.graph.get_barrier(task_id)
            barrier = len(inbounds) if barrier == '*' else barrier

        for prev_transition in inbounds:
            prev_task_state_entry = self.get_task_state_entry(prev_transition[0], route)

            if prev_task_state_entry:
                prev_task_transition_id = (
                    constants.TASK_STATE_TRANSITION_FORMAT %
                    (prev_transition[1], str(prev_transition[2]))
                )

                if (prev_task_transition_id in prev_task_state_entry['next'] and
                        prev_task_state_entry['next'][prev_task_transition_id]):
                    inbounds_satisfied.append(prev_task_transition_id)

        return (len(inbounds_satisfied) >= barrier)

    def get_task(self, task_id, route):
        try:
            task_ctx = self.get_task_initial_context(task_id, route)
        except ValueError:
            task_ctx = self.get_workflow_initial_context()

        state_ctx = {'__state': self.workflow_state.serialize()}
        current_task = {'id': task_id, 'route': route}
        task_ctx = ctx_util.set_current_task(task_ctx, current_task)
        task_ctx = dict_util.merge_dicts(task_ctx, state_ctx, True)
        task_spec = self.spec.tasks.get_task(task_id).copy()
        task_spec, action_specs = task_spec.render(task_ctx)

        task = {
            'id': task_id,
            'route': route,
            'ctx': task_ctx,
            'spec': task_spec,
            'actions': action_specs
        }

        # If there is a task delay specified, evaluate the delay value.
        if getattr(task_spec, 'delay', None):
            task_delay = task_spec.delay

            if isinstance(task_delay, six.string_types):
                task_delay = expr_base.evaluate(task_delay, task_ctx)

            if not isinstance(task_delay, int):
                raise TypeError('The value of task delay is not type of integer.')

            task['delay'] = task_delay

        # Add items and related meta data to the task details.
        if task_spec.has_items():
            items_spec = getattr(task_spec, 'with')
            concurrency = getattr(items_spec, 'concurrency', None)
            task['items_count'] = len(action_specs)
            task['concurrency'] = expr_base.evaluate(concurrency, task_ctx)

        return task

    def _evaluate_task_actions(self, task):
        task_id = task['id']
        task_route = task['route']

        # Return task if it is not with items.
        if not task['spec'].has_items():
            return task

        # Fetch the task entry from staging.
        staged_task = self.workflow_state.get_staged_task(task_id, task_route)

        # Prepare the staging task to track items execution status.
        if 'items' not in staged_task or not staged_task['items']:
            staged_task['items'] = [{'status': statuses.UNSET}] * task['items_count']

        # Trim the list of actions in the task per concurrency policy.
        all_items = list(zip(task['actions'], staged_task['items']))
        notrun_items = list(filter(lambda x: x[1]['status'] == statuses.UNSET, all_items))
        active_items = list(filter(lambda x: x[1]['status'] in statuses.ACTIVE_STATUSES, all_items))

        if task['concurrency'] is not None:
            availability = task['concurrency'] - len(active_items)
            candidates = list(zip(*notrun_items[:availability]))
            task['actions'] = list(candidates[0]) if candidates and availability > 0 else []
        else:
            candidates = list(zip(*notrun_items))
            task['actions'] = list(candidates[0]) if candidates else []

        return task

    def has_next_tasks(self, task_id=None, route=None):
        if not task_id:
            return True if self.workflow_state.get_staged_tasks() else False
        else:
            task_state_entry = self.get_task_state_entry(task_id, route)

            if (not task_state_entry or
                    task_state_entry.get('status') not in statuses.COMPLETED_STATUSES):
                return []

            outbounds = self.graph.get_next_transitions(task_id)

            for next_seq in outbounds:
                next_task_id, seq_key = next_seq[1], next_seq[2]
                task_transition_id = (
                    constants.TASK_STATE_TRANSITION_FORMAT %
                    (next_task_id, str(seq_key))
                )

                # Evaluate if outbound criteria is satisfied.
                if not task_state_entry['next'].get(task_transition_id):
                    continue

                # Evaluate if inbound criteria for the next task is satisfied.
                if not self._inbound_criteria_satisfied(next_task_id, route):
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
            remediation_tasks = [s for s in staged_tasks if s.get('run_on_fail', False) is True]

        # Return an empty list if the workflow is not running and there is no remediation tasks.
        if self.get_workflow_status() not in statuses.RUNNING_STATUSES and not remediation_tasks:
            return next_tasks

        # Return the list of tasks that are staged and readied. If there is exception on
        # task rendering, then log the error and continue. This allows user to know about
        # all task rendering errors for this task transition instead of getting rendering
        # error one at a time during runtime.
        for staged_task in remediation_tasks or staged_tasks:
            try:
                next_task = self.get_task(staged_task['id'], staged_task['route'])
                next_task = self._evaluate_task_actions(next_task)

                if 'actions' in next_task and len(next_task['actions']) > 0:
                    next_tasks.append(next_task)
                elif 'items_count' in next_task and next_task['items_count'] == 0:
                    next_tasks.append(next_task)
            except Exception as e:
                fail_on_task_rendering = True
                self.log_error(e, task_id=staged_task['id'], route=staged_task['route'])
                continue

        # Return nothing if there is error(s) on determining next tasks.
        if fail_on_task_rendering:
            self.request_workflow_status(statuses.FAILED)
            return []

        return sorted(next_tasks, key=lambda x: (x['id'], x['route']))

    def _get_task_state_idx(self, task_id, route):
        return self.workflow_state.tasks.get(
            constants.TASK_STATE_ROUTE_FORMAT % (task_id, str(route))
        )

    def get_task_state_entry(self, task_id, route):
        task_state_seq_idx = self._get_task_state_idx(task_id, route)

        if task_state_seq_idx is None:
            return None

        return self.workflow_state.sequence[task_state_seq_idx]

    def add_task_state(self, task_id, route, in_ctx_idxs=None, prev=None):
        if not self.graph.has_task(task_id):
            raise exc.InvalidTask(task_id)

        if not in_ctx_idxs:
            in_ctx_idxs = [0]

        task_state_entry = {
            'id': task_id,
            'route': route,
            'ctxs': {
                'in': in_ctx_idxs
            },
            'prev': prev or {},
            'next': {}
        }

        task_state_entry_id = constants.TASK_STATE_ROUTE_FORMAT % (task_id, str(route))
        self.workflow_state.sequence.append(task_state_entry)
        self.workflow_state.tasks[task_state_entry_id] = len(self.workflow_state.sequence) - 1

        return task_state_entry

    def update_task_state(self, task_id, route, event):
        engine_event_queue = queue.Queue()

        # Throw exception if not expected event type.
        if not issubclass(type(event), events.ExecutionEvent):
            raise TypeError('Event is not type of ExecutionEvent.')

        # Throw exception if task does not exist in the workflow graph.
        if not self.graph.has_task(task_id):
            raise exc.InvalidTask(task_id)

        # Try to get the task metadata from staging or task state.
        staged_task = self.workflow_state.get_staged_task(task_id, route)
        task_state_entry = self.get_task_state_entry(task_id, route)

        # Throw exception if task is not staged and there is no task state entry.
        if not staged_task and not task_state_entry:
            raise exc.InvalidTaskStateEntry(task_id)

        # Create new task state entry if it does not exist.
        if not task_state_entry:
            task_state_entry = self.add_task_state(
                task_id,
                staged_task['route'],
                in_ctx_idxs=staged_task['ctxs']['in'],
                prev=staged_task['prev']
            )

        # Identify the index for the task state object for later use.
        task_state_idx = self._get_task_state_idx(task_id, route)

        # If task is already completed and in cycle, then create new task state entry.
        if (self.graph.in_cycle(task_id) and
                task_state_entry.get('status') in statuses.COMPLETED_STATUSES):
            task_state_entry = self.add_task_state(
                task_id,
                staged_task['route'],
                in_ctx_idxs=staged_task['ctxs']['in'],
                prev=staged_task['prev']
            )

            # Update the index value since a new entry is created.
            task_state_idx = self._get_task_state_idx(task_id, route)

        # Remove task from staging if task is not with items.
        if event.status and staged_task and 'items' not in staged_task:
            self.workflow_state.remove_staged_task(task_id, route)

        # If action execution is for a task item, then store the execution status for the item.
        if (staged_task and event.status and event.context and
                'item_id' in event.context and event.context['item_id'] is not None):
            item_result = {'status': event.status, 'result': event.result}
            staged_task['items'][event.context['item_id']] = item_result

        # Log the error if it is a failed execution event.
        if event.status == statuses.FAILED:
            message = 'Execution failed. See result for details.'
            self.log_entry('error', message, task_id=task_id, result=event.result)

        # Process the action execution event using the
        # task state machine and update the task status.
        old_task_status = task_state_entry.get('status', statuses.UNSET)
        machines.TaskStateMachine.process_event(self.workflow_state, task_state_entry, event)
        new_task_status = task_state_entry.get('status', statuses.UNSET)

        # Get task result and set current context if task is completed.
        if new_task_status in statuses.COMPLETED_STATUSES:
            # Get task details required for updating outgoing context.
            task_spec = self.spec.tasks.get_task(task_id)

            # Get task result.
            task_result = (
                [item.get('result') for item in staged_task.get('items', [])]
                if staged_task and task_spec.has_items() else event.result
            )

            # Remove remaining task from staging.
            self.workflow_state.remove_staged_task(task_id, route)

            # Set current task in the context.
            in_ctx_idxs = task_state_entry['ctxs']['in']
            in_ctx_val = self.get_task_context(in_ctx_idxs)
            current_task = {'id': task_id, 'route': route, 'result': task_result}
            current_ctx = ctx_util.set_current_task(in_ctx_val, current_task)

            # Setup context for evaluating expressions in task transition criteria.
            state_ctx = {'__state': self.workflow_state.serialize()}
            current_ctx = dict_util.merge_dicts(current_ctx, state_ctx, True)

        # Evaluate task transitions if task is completed and status change is not processed.
        if new_task_status in statuses.COMPLETED_STATUSES and new_task_status != old_task_status:
            has_manual_fail = False
            staged_next_tasks = []

            # Identify task transitions for the current completed task.
            task_transitions = self.graph.get_next_transitions(task_id)

            # Mark task as terminal when there is no transitions.
            if not task_transitions:
                task_state_entry['term'] = True

            # Iterate thru each outbound task transitions.
            for task_transition in task_transitions:
                task_transition_id = (
                    constants.TASK_STATE_TRANSITION_FORMAT %
                    (task_transition[1], str(task_transition[2]))
                )

                # Evaluate the criteria for task transition. If there is a failure while
                # evaluating expression(s), fail the workflow.
                try:
                    criteria = task_transition[3].get('criteria') or []
                    evaluated_criteria = [expr_base.evaluate(c, current_ctx) for c in criteria]
                    task_state_entry['next'][task_transition_id] = all(evaluated_criteria)
                except Exception as e:
                    self.log_error(e, task_id, route, task_transition_id)
                    self.request_workflow_status(statuses.FAILED)
                    continue

                # If criteria met, then mark the next task staged and calculate outgoing context.
                if task_state_entry['next'][task_transition_id]:
                    next_task_node = self.graph.get_task(task_transition[1])
                    next_task_id = next_task_node['id']
                    new_ctx_idx = None

                    # Get and process new context for the task transition.
                    out_ctx, new_ctx, errors = task_spec.finalize_context(
                        next_task_id,
                        task_transition,
                        copy.deepcopy(current_ctx)
                    )

                    if errors:
                        self.log_errors(errors, task_id, route, task_transition_id)
                        self.request_workflow_status(statuses.FAILED)
                        continue

                    out_ctx_idxs = copy.deepcopy(task_state_entry['ctxs']['in'])

                    if new_ctx:
                        self.workflow_state.contexts.append(new_ctx)
                        new_ctx_idx = len(self.workflow_state.contexts) - 1

                        # Add to the list of contexts for the next task in this transition.
                        out_ctx_idxs.append(new_ctx_idx)

                        # Record the outgoing context for this task transition.
                        if 'out' not in task_state_entry['ctxs']:
                            task_state_entry['ctxs']['out'] = {}

                        task_state_entry['ctxs']['out'] = {task_transition_id: new_ctx_idx}

                    # Stage the next task if it is not in staging.
                    next_task_route = self._evaluate_route(task_transition, route)

                    staged_next_task = self.workflow_state.get_staged_task(
                        next_task_id,
                        next_task_route
                    )

                    backref = (
                        constants.TASK_STATE_TRANSITION_FORMAT %
                        (task_id, str(task_transition[2]))
                    )

                    # If the next task is already staged.
                    if staged_next_task:
                        # Remove the root context to avoid overwriting vars.
                        out_ctx_idxs.remove(0)

                        # Extend the outgoing context from this task.
                        staged_next_task['ctxs']['in'].extend(out_ctx_idxs)

                        # Add a backref for the current task in the next task.
                        staged_next_task['prev'][backref] = task_state_idx
                    else:
                        # Otherwise create a new entry in staging for the next task.
                        staged_next_task = self.workflow_state.add_staged_task(
                            next_task_id,
                            next_task_route,
                            ctxs=out_ctx_idxs,
                            prev={backref: task_state_idx},
                            ready=False
                        )

                    # Check if inbound criteria are met. Must use the original route
                    # to identify the inbound task transitions.
                    staged_next_task['ready'] = self._inbound_criteria_satisfied(
                        next_task_id,
                        route
                    )

                    # Put the next task in the engine event queue if it is an engine command.
                    if next_task_id in events.ENGINE_EVENT_MAP.keys():
                        queue_entry = (staged_next_task['id'], staged_next_task['route'])
                        engine_event_queue.put(queue_entry)

                        # Flag if there is at least one fail command in the task transition.
                        if not has_manual_fail:
                            has_manual_fail = (next_task_id == 'fail')
                    else:
                        # If not an engine command and the next task is ready, then
                        # make a record of it for processing manual fail below.
                        if staged_next_task['ready']:
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
                    staged_next_task['run_on_fail'] = True

        # Process the task event using the workflow state machine and update the workflow status.
        task_ex_event = events.TaskExecutionEvent(task_id, route, task_state_entry['status'])
        machines.WorkflowStateMachine.process_event(self.workflow_state, task_ex_event)

        # Process any engine commands in the queue.
        while not engine_event_queue.empty():
            next_task_id, next_task_route = engine_event_queue.get()
            engine_event = events.ENGINE_EVENT_MAP[next_task_id]
            self.update_task_state(next_task_id, next_task_route, engine_event())

        # Render workflow output if workflow is completed.
        if self.get_workflow_status() in statuses.COMPLETED_STATUSES:
            task_state_entry['term'] = True
            self._render_workflow_outputs()

        return task_state_entry

    def _evaluate_route(self, task_transition, prev_route):
        task_id = task_transition[1]

        prev_task_transition_id = (
            constants.TASK_STATE_TRANSITION_FORMAT %
            (task_transition[0], str(task_transition[2]))
        )

        is_split_task = self.spec.tasks.is_split_task(task_id)
        is_in_cycle = self.graph.in_cycle(task_id)

        if not is_split_task or is_in_cycle:
            return prev_route

        old_route_details = self.workflow_state.routes[prev_route]
        new_route_details = copy.deepcopy(old_route_details)

        if prev_task_transition_id not in old_route_details:
            new_route_details.append(prev_task_transition_id)

        if old_route_details == new_route_details:
            return prev_route

        self.workflow_state.routes.append(new_route_details)

        return len(self.workflow_state.routes) - 1

    def get_task_context(self, ctx_idxs):
        ctx = {}

        for ctx_idx in ctx_idxs:
            ctx = dict_util.merge_dicts(ctx, self.workflow_state.contexts[ctx_idx], overwrite=True)

        return ctx

    def get_task_initial_context(self, task_id, route):
        staged_task = self.workflow_state.get_staged_task(task_id, route)

        if staged_task:
            return self.get_task_context(staged_task['ctxs']['in'])

        task_state_entry = self.get_task_state_entry(task_id, route)

        if task_state_entry:
            return self.get_task_context(task_state_entry['ctxs']['in'])

        raise ValueError('Unable to determine context for task "%s".' % task_id)

    def get_task_transition_contexts(self, task_id, route):
        contexts = {}

        task_state_entry = self.get_task_state_entry(task_id, route)

        if not task_state_entry:
            raise exc.InvalidTaskStateEntry(task_id)

        for t in self.graph.get_next_transitions(task_id):
            task_transition_id = constants.TASK_STATE_TRANSITION_FORMAT % (t[1], str(t[2]))

            if (task_transition_id in task_state_entry['next'] and
                    task_state_entry['next'][task_transition_id]):
                contexts[task_transition_id] = self.get_task_initial_context(t[1], route)

        return contexts
