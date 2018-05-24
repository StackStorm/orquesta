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

from orchestra import exceptions as exc
from orchestra.expressions import base as expr
from orchestra import graphing
from orchestra.specs import base as specs
from orchestra.specs import loader as specs_loader
from orchestra import states
from orchestra.utils import context as ctx
from orchestra.utils import dictionary as dx
from orchestra.utils import plugin


LOG = logging.getLogger(__name__)


class TaskFlow(object):

    def __init__(self):
        self.tasks = dict()
        self.sequence = list()
        self.contexts = list()
        self.staged = dict()

    def serialize(self):
        return {
            'tasks': copy.deepcopy(self.tasks),
            'sequence': copy.deepcopy(self.sequence),
            'contexts': copy.deepcopy(self.contexts),
            'staged': copy.deepcopy(self.staged)
        }

    @classmethod
    def deserialize(cls, data):
        instance = cls()
        instance.tasks = copy.deepcopy(data.get('tasks', dict()))
        instance.sequence = copy.deepcopy(data.get('sequence', list()))
        instance.contexts = copy.deepcopy(data.get('contexts', list()))
        instance.staged = copy.deepcopy(data.get('staged', dict()))

        return instance


class WorkflowConductor(object):

    def __init__(self, spec, **kwargs):
        if not spec or not isinstance(spec, specs.Spec):
            raise ValueError('The value of "spec" is not type of Spec.')

        self.spec = spec
        self.catalog = self.spec.get_catalog()
        self.spec_module = specs_loader.get_spec_module(self.catalog)
        self.composer = plugin.get_module('orchestra.composers', self.catalog)

        self._workflow_state = None
        self._graph = None
        self._flow = None
        self._inputs = kwargs
        self._outputs = None
        self._errors = []

    def restore(self, graph, state=None, errors=None, flow=None, inputs=None, outputs=None):
        if not graph or not isinstance(graph, graphing.WorkflowGraph):
            raise ValueError('The value of "graph" is not type of WorkflowGraph.')

        if not flow or not isinstance(flow, TaskFlow):
            raise ValueError('The value of "flow" is not type of TaskFlow.')

        if state and not states.is_valid(state):
            raise exc.InvalidState(state)

        if inputs is not None and not isinstance(inputs, dict):
            raise ValueError('The value of "inputs" is not type of dict.')

        if outputs is not None and not isinstance(outputs, dict):
            raise ValueError('The value of "outputs" is not type of dict.')

        self._workflow_state = state
        self._graph = graph
        self._flow = flow
        self._inputs = inputs or {}
        self._outputs = outputs
        self._errors = errors or []

    def serialize(self):
        return {
            'spec': self.spec.serialize(),
            'graph': self.graph.serialize(),
            'state': self.get_workflow_state(),
            'flow': self.flow.serialize(),
            'inputs': self.get_workflow_input(),
            'outputs': self.get_workflow_output(),
            'errors': copy.deepcopy(self.errors)
        }

    @classmethod
    def deserialize(cls, data):
        spec_module = specs_loader.get_spec_module(data['spec']['catalog'])
        spec = spec_module.WorkflowSpec.deserialize(data['spec'])

        graph = graphing.WorkflowGraph.deserialize(data['graph'])
        state = data['state']
        flow = TaskFlow.deserialize(data['flow'])
        inputs = copy.deepcopy(data['inputs'])
        outputs = copy.deepcopy(data['outputs'])
        errors = copy.deepcopy(data['errors'])

        instance = cls(spec)
        instance.restore(graph, state, errors, flow, inputs, outputs)

        return instance

    @property
    def graph(self):
        if not self._graph:
            self._graph = self.composer.compose(self.spec)

        return self._graph

    @property
    def flow(self):
        if not self._flow:
            self._flow = TaskFlow()

            # Render runtime and default workflow inputs and vars.
            spec_inputs = self.spec.input or []
            default_inputs = dict([list(i.items())[0] for i in spec_inputs if isinstance(i, dict)])
            merged_inputs = dx.merge_dicts(default_inputs, self.get_workflow_input(), True)

            try:
                rendered_inputs = expr.evaluate(merged_inputs, {})
                rendered_vars = expr.evaluate(getattr(self.spec, 'vars', {}), rendered_inputs)
            except exc.ExpressionEvaluationException as e:
                self.log_error(str(e))
                self.set_workflow_state(states.FAILED)

            # Proceed if there is no issue with rendering of inputs and vars.
            if self.get_workflow_state() not in states.ABENDED_STATES:
                # Set the initial workflow context.
                ctx_value = dx.merge_dicts(rendered_inputs, rendered_vars)
                self._flow.contexts.append({'srcs': [], 'value': ctx_value})

                # Identify the starting tasks and set the pointer to the initial context entry.
                for task_node in self.graph.roots:
                    self._flow.staged[task_node['id']] = {'ctxs': [0]}

        return self._flow

    @property
    def errors(self):
        return self._errors

    def log_error(self, error, task_id=None, task_transition_id=None):
        entry = {'message': error}

        if task_id:
            entry['task_id'] = task_id

        if task_transition_id:
            entry['task_transition_id'] = task_transition_id

        self.errors.append(entry)

    def get_workflow_input(self):
        return copy.deepcopy(self._inputs)

    def get_workflow_state(self):
        return self._workflow_state

    def set_workflow_state(self, value):
        if not states.is_valid(value):
            raise exc.InvalidState(value)

        if not states.is_transition_valid(self._workflow_state, value):
            raise exc.InvalidStateTransition(self._workflow_state, value)

        # Determine if workflow is pausing or paused or canceling or canceled.
        if value in states.PAUSE_STATES or value in states.CANCEL_STATES:
            any_active_tasks = any([t['state'] in states.ACTIVE_STATES for t in self.flow.sequence])
            value = states.PAUSING if any_active_tasks and value == states.PAUSED else value
            value = states.PAUSED if not any_active_tasks and value == states.PAUSING else value
            value = states.CANCELING if any_active_tasks and value == states.CANCELED else value
            value = states.CANCELED if not any_active_tasks and value == states.CANCELING else value

        self._workflow_state = value

    def get_workflow_initial_context(self):
        return copy.deepcopy(self.flow.contexts[0])

    def get_workflow_terminal_context_idx(self):
        query = filter(lambda x: 'term' in x[1] and x[1]['term'], enumerate(self.flow.contexts))
        match = list(query)

        if not match or len(match) <= 0:
            return None

        if match and len(match) != 1:
            raise Exception('More than one final workflow context found.')

        return match[0][0]

    def get_workflow_terminal_context(self):
        if self.get_workflow_state() not in states.COMPLETED_STATES:
            raise Exception('Workflow is not in completed state.')

        term_ctx_idx = self.get_workflow_terminal_context_idx()

        if not term_ctx_idx:
            raise Exception('Unable to determine the final workflow context.')

        return copy.deepcopy(self.flow.contexts[term_ctx_idx])

    def update_workflow_terminal_context(self, ctx_diff, task_flow_idx):
        term_ctx_idx = self.get_workflow_terminal_context_idx()

        if not term_ctx_idx:
            term_ctx_val = copy.deepcopy(ctx_diff)
            term_ctx_entry = {'src': [task_flow_idx], 'term': True, 'value': term_ctx_val}
            self.flow.contexts.append(term_ctx_entry)
            term_ctx_idx = len(self.flow.contexts) - 1
        else:
            term_ctx_entry = self.flow.contexts[term_ctx_idx]
            if task_flow_idx not in term_ctx_entry['src']:
                term_ctx_val = dx.merge_dicts(term_ctx_entry['value'], ctx_diff, True)
                term_ctx_entry['src'].append(task_flow_idx)
                term_ctx_entry['value'] = term_ctx_val

    def render_workflow_outputs(self):
        if self.get_workflow_state() == states.SUCCEEDED and not self._outputs:
            wf_output_spec = getattr(self.spec, 'output') or {}
            wf_term_ctx = self.get_workflow_terminal_context()

            try:
                self._outputs = {
                    var_name: expr.evaluate(var_expr, wf_term_ctx['value'])
                    for var_name, var_expr in six.iteritems(wf_output_spec)
                }
            except exc.ExpressionEvaluationException as e:
                self.log_error(str(e))
                self.set_workflow_state(states.FAILED)

    def get_workflow_output(self):
        return copy.deepcopy(self._outputs) if self._outputs else None

    def render_task_spec(self, task_name, ctx_value):
        task_spec = self.spec.tasks.get_task(task_name).copy()
        task_spec.action = expr.evaluate(task_spec.action, ctx_value)
        task_spec.input = expr.evaluate(getattr(task_spec, 'input', {}), ctx_value)
        return task_spec

    def get_task(self, task_id):
        task_node = self.graph.get_task(task_id)
        task_name = task_node['name']

        try:
            task_ctx = self.get_task_initial_context(task_id)['value']
        except ValueError:
            task_ctx = self.get_workflow_initial_context()

        task_spec = self.render_task_spec(task_name, task_ctx)

        return {
            'id': task_id,
            'name': task_name,
            'ctx': task_ctx,
            'spec': task_spec
        }

    def get_start_tasks(self):
        if self.get_workflow_state() not in states.RUNNING_STATES:
            return []

        tasks = []

        for task_node in self.graph.roots:
            try:
                tasks.append(self.get_task(task_node['id']))
            except exc.ExpressionEvaluationException as e:
                self.log_error(str(e), task_id=task_node['id'])
                self.set_workflow_state(states.FAILED)
                continue

        # Return nothing if there is error(s) on determining start tasks.
        if self.get_workflow_state() in states.COMPLETED_STATES:
            return []

        return sorted(tasks, key=lambda x: x['name'])

    def get_next_tasks(self, task_id=None):
        if self.get_workflow_state() not in states.RUNNING_STATES:
            return []

        next_tasks = []

        if not task_id:
            for staged_task_id in self.flow.staged.keys():
                try:
                    next_tasks.append(self.get_task(staged_task_id))
                except exc.ExpressionEvaluationException as e:
                    self.log_error(str(e), task_id=staged_task_id)
                    self.set_workflow_state(states.FAILED)
                    continue
        else:
            task_flow_entry = self.get_task_flow_entry(task_id)

            if not task_flow_entry or task_flow_entry.get('state') not in states.COMPLETED_STATES:
                return []

            outbounds = self.graph.get_next_transitions(task_id)

            for next_seq in outbounds:
                next_task_id, seq_key = next_seq[1], next_seq[2]
                task_transition_id = next_task_id + '__' + str(seq_key)

                # Evaluate if outbound criteria is satisfied.
                if not task_flow_entry.get(task_transition_id):
                    continue

                # Evaluate if the next task has a barrier waiting for other tasks to complete.
                if self.graph.has_barrier(next_task_id):
                    barrier = self.graph.get_barrier(next_task_id)
                    inbounds = self.graph.get_prev_transitions(next_task_id)

                    barrier = len(inbounds) if barrier == '*' else barrier
                    satisfied = []

                    for prev_seq in inbounds:
                        prev_task_flow_entry = self.get_task_flow_entry(prev_seq[0])

                        if prev_task_flow_entry:
                            prev_task_transition_id = prev_seq[1] + '__' + str(prev_seq[2])

                            if prev_task_flow_entry.get(prev_task_transition_id):
                                satisfied.append(prev_task_transition_id)

                    if len(satisfied) < barrier:
                        continue

                next_task_node = self.graph.get_task(next_task_id)

                # If the next task is named noop which is a reserved task name, then skip the task.
                if next_task_node['name'] == 'noop':
                    continue

                try:
                    next_tasks.append(self.get_task(next_task_id))
                except exc.ExpressionEvaluationException as e:
                    self.log_error(str(e), task_id=next_task_id)
                    self.set_workflow_state(states.FAILED)
                    continue

        # Return nothing if there is error(s) on determining next tasks.
        if self.get_workflow_state() in states.COMPLETED_STATES:
            return []

        return sorted(next_tasks, key=lambda x: x['name'])

    def get_task_flow_idx(self, task_id):
        return self.flow.tasks.get(task_id)

    def get_task_flow_entry(self, task_id):
        flow_idx = self.get_task_flow_idx(task_id)

        return self.flow.sequence[flow_idx] if flow_idx is not None else None

    def add_task_flow(self, task_id, in_ctx_idx=None):
        if not self.graph.has_task(task_id):
            raise exc.InvalidTask(task_id)

        task_flow_entry = {'id': task_id, 'ctx': in_ctx_idx}
        self.flow.sequence.append(task_flow_entry)
        self.flow.tasks[task_id] = len(self.flow.sequence) - 1

        return task_flow_entry

    def update_task_flow(self, task_id, state, result=None):
        in_ctx_idx = 0

        if not states.is_valid(state):
            raise exc.InvalidState(state)

        # Remove the task from the staged list if it becomes active.
        if state in states.ACTIVE_STATES and task_id in self.flow.staged:
            in_ctx_idxs = self.flow.staged[task_id]['ctxs']

            if len(in_ctx_idxs) <= 0 or all(x == in_ctx_idxs[0] for x in in_ctx_idxs):
                in_ctx_idx = in_ctx_idxs[0]
            else:
                new_ctx_entry = self.converge_task_contexts(in_ctx_idxs)
                self.flow.contexts.append(new_ctx_entry)
                in_ctx_idx = len(self.flow.contexts) - 1

            del self.flow.staged[task_id]

        # Get task flow entry and create new if it does not exist.
        task_flow_entry = self.get_task_flow_entry(task_id)

        if not task_flow_entry:
            task_flow_entry = self.add_task_flow(task_id, in_ctx_idx=in_ctx_idx)

        # If task is alstaged completed and in cycle, then create new task flow entry.
        if self.graph.in_cycle(task_id) and task_flow_entry.get('state') in states.COMPLETED_STATES:
            task_flow_entry = self.add_task_flow(task_id, in_ctx_idx=in_ctx_idx)

        # If the task state change is valid, update state in task flow entry.
        if not states.is_transition_valid(task_flow_entry.get('state'), state):
            raise exc.InvalidStateTransition(task_flow_entry.get('state'), state)

        task_flow_entry['state'] = state

        # Evaluate task transitions if task is in completed state.
        if state in states.COMPLETED_STATES:
            # Get task details required for updating outgoing context.
            task_node = self.graph.get_task(task_id)
            task_name = task_node['name']
            task_spec = self.spec.tasks.get_task(task_name)
            task_flow_idx = self.get_task_flow_idx(task_id)

            # Set current task in the context.
            in_ctx_idx = task_flow_entry['ctx']
            in_ctx_val = self.flow.contexts[in_ctx_idx]['value']
            current_task = {'id': task_id, 'name': task_name, 'result': result}
            current_ctx = ctx.set_current_task(in_ctx_val, current_task)

            # Setup context for evaluating expressions in task transition criteria.
            flow_ctx = {'__flow': self.flow.serialize()}
            current_ctx = dx.merge_dicts(current_ctx, flow_ctx, True)

            # Identify task transitions for the current completed task.
            task_transitions = self.graph.get_next_transitions(task_id)

            # Update workflow context when there is no transitions.
            if not task_transitions:
                self.update_workflow_terminal_context(in_ctx_val, task_flow_idx)

            # Iterate thru each outbound task transitions.
            for task_transition in task_transitions:
                task_transition_id = task_transition[1] + '__' + str(task_transition[2])

                # Evaluate the criteria for task transition. If there is a failure while
                # evaluating expression(s), fail the workflow.
                try:
                    criteria = task_transition[3].get('criteria') or []
                    evaluated_criteria = [expr.evaluate(c, current_ctx) for c in criteria]
                    task_flow_entry[task_transition_id] = all(evaluated_criteria)
                except exc.ExpressionEvaluationException as e:
                    self.log_error(str(e), task_id, task_transition_id)
                    self.set_workflow_state(states.FAILED)
                    continue

                # If criteria met, then mark the next task staged and calculate outgoing context.
                if task_flow_entry[task_transition_id]:
                    next_task_node = self.graph.get_task(task_transition[1])
                    next_task_name = next_task_node['name']

                    try:
                        out_ctx_val = task_spec.finalize_context(
                            next_task_name,
                            criteria,
                            copy.deepcopy(current_ctx)
                        )
                    except exc.ExpressionEvaluationException as e:
                        self.log_error(str(e), task_id, task_transition_id)
                        self.set_workflow_state(states.FAILED)
                        continue

                    if out_ctx_val != in_ctx_val:
                        task_flow_idx = self.get_task_flow_idx(task_id)
                        self.flow.contexts.append({'srcs': [task_flow_idx], 'value': out_ctx_val})
                        out_ctx_idx = len(self.flow.contexts) - 1
                    else:
                        out_ctx_idx = in_ctx_idx

                    if (task_transition[1] in self.flow.staged and
                            'ctxs' in self.flow.staged[task_transition[1]]):
                        self.flow.staged[task_transition[1]]['ctxs'].append(out_ctx_idx)
                    else:
                        self.flow.staged[task_transition[1]] = {'ctxs': [out_ctx_idx]}

                    # If the next task is noop, then mark the task as completed.
                    if next_task_name == 'noop':
                        next_task_id = next_task_node['id']
                        self.update_task_flow(next_task_id, states.RUNNING)
                        self.update_task_flow(next_task_id, states.SUCCEEDED)

                    # If the next task is fail, then fail the workflow..
                    if next_task_name == 'fail':
                        next_task_id = next_task_node['id']
                        self.update_task_flow(next_task_id, states.RUNNING)
                        self.update_task_flow(next_task_id, states.FAILED)

        # Identify if there are task transitions.
        any_next_tasks = False

        for t in self.graph.get_next_transitions(task_id):
            task_transition_id = t[1] + '__' + str(t[2])
            any_next_tasks = task_flow_entry.get(task_transition_id, False)
            if any_next_tasks:
                break

        # Identify if there are any other active tasks.
        any_active_tasks = any([t['state'] in states.ACTIVE_STATES for t in self.flow.sequence])

        # Identify if there are any other staged tasks.
        any_staged_tasks = len(self.flow.staged) > 0

        # Update workflow state.
        old_state = self.get_workflow_state()
        new_state = self.get_workflow_state()

        if task_flow_entry['state'] == states.RESUMING:
            new_state = states.RESUMING
        elif task_flow_entry['state'] in states.RUNNING_STATES:
            new_state = states.RUNNING
        elif task_flow_entry['state'] in states.PAUSE_STATES and old_state == states.CANCELING:
            new_state = states.CANCELING if any_active_tasks else states.CANCELED
        elif task_flow_entry['state'] in states.PAUSE_STATES:
            new_state = states.PAUSING if any_active_tasks else states.PAUSED
        elif task_flow_entry['state'] in states.CANCEL_STATES:
            new_state = states.CANCELING if any_active_tasks else states.CANCELED
        elif task_flow_entry['state'] in states.COMPLETED_STATES and old_state == states.PAUSING:
            new_state = states.PAUSING if any_active_tasks else states.PAUSED
        elif task_flow_entry['state'] in states.COMPLETED_STATES and old_state == states.CANCELING:
            new_state = states.CANCELING if any_active_tasks else states.CANCELED
        elif task_flow_entry['state'] in states.ABENDED_STATES:
            new_state = states.RUNNING if any_next_tasks else states.FAILED
        elif task_flow_entry['state'] == states.SUCCEEDED:
            is_wf_running = any_active_tasks or any_next_tasks or any_staged_tasks
            new_state = states.RUNNING if is_wf_running else states.SUCCEEDED

        if old_state != new_state and states.is_transition_valid(old_state, new_state):
            self.set_workflow_state(new_state)

        if self.get_workflow_state() in states.COMPLETED_STATES:
            in_ctx_idx = task_flow_entry['ctx']
            in_ctx_val = self.flow.contexts[in_ctx_idx]['value']
            task_flow_idx = self.get_task_flow_idx(task_id)
            self.update_workflow_terminal_context(in_ctx_val, task_flow_idx)
            self.render_workflow_outputs()

        return task_flow_entry

    def converge_task_contexts(self, ctx_idxs):
        if len(ctx_idxs) <= 0 or all(x == ctx_idxs[0] for x in ctx_idxs):
            return self.flow.contexts[ctx_idxs[0]]

        ctx_srcs = []
        merged_ctx = {}

        for i in ctx_idxs:
            ctx_entry = self.flow.contexts[i]
            merged_ctx = dx.merge_dicts(merged_ctx, copy.deepcopy(ctx_entry['value']), True)
            ctx_srcs.extend(ctx_entry['srcs'])

        return {'srcs': list(set(ctx_srcs)), 'value': merged_ctx}

    def get_task_initial_context(self, task_id):
        task_flow_entry = self.get_task_flow_entry(task_id)

        if task_flow_entry:
            in_ctx_idx = task_flow_entry.get('ctx')
            return copy.deepcopy(self.flow.contexts[in_ctx_idx])

        if task_id in self.flow.staged:
            in_ctx_idxs = self.flow.staged[task_id]['ctxs']
            return self.converge_task_contexts(in_ctx_idxs)

        raise ValueError('Unable to determine context for task "%s".' % task_id)

    def get_task_transition_contexts(self, task_id):
        contexts = {}

        task_flow_entry = self.get_task_flow_entry(task_id)

        if not task_flow_entry:
            raise Exception('Task "%s" is not staged or has not started yet.' % task_id)

        for t in self.graph.get_next_transitions(task_id):
            task_transition_id = t[1] + '__' + str(t[2])

            if task_transition_id in task_flow_entry and task_flow_entry[task_transition_id]:
                contexts[task_transition_id] = self.get_task_initial_context(t[1])

        return contexts
