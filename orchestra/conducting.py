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

    def restore(self, graph, state=None, flow=None, inputs=None):
        if not graph or not isinstance(graph, graphing.WorkflowGraph):
            raise ValueError('The value of "graph" is not type of WorkflowGraph.')

        if not flow or not isinstance(flow, TaskFlow):
            raise ValueError('The value of "flow" is not type of TaskFlow.')

        if state and not states.is_valid(state):
            raise exc.InvalidState(state)

        if inputs is not None and not isinstance(inputs, dict):
            raise ValueError('The value of "inputs" is not type of dict.')

        self._workflow_state = state
        self._graph = graph
        self._flow = flow
        self._inputs = inputs or {}

    def serialize(self):
        return {
            'spec': self.spec.serialize(),
            'graph': self.graph.serialize(),
            'state': self.state,
            'flow': self.flow.serialize(),
            'inputs': copy.deepcopy(self.inputs)
        }

    @classmethod
    def deserialize(cls, data):
        spec_module = specs_loader.get_spec_module(data['spec']['catalog'])
        spec = spec_module.WorkflowSpec.deserialize(data['spec'])

        graph = graphing.WorkflowGraph.deserialize(data['graph'])
        state = data['state']
        flow = TaskFlow.deserialize(data['flow'])
        inputs = copy.deepcopy(data['inputs'])

        instance = cls(spec)
        instance.restore(graph, state, flow, inputs)

        return instance

    @property
    def state(self):
        return self._workflow_state

    @property
    def graph(self):
        if not self._graph:
            self._graph = self.composer.compose(self.spec)

        return self._graph

    @property
    def flow(self):
        if not self._flow:
            self._flow = TaskFlow()

            # Get default workflow inputs.
            spec_inputs = self.spec.input or []
            default_inputs = dict([list(i.items())[0] for i in spec_inputs if isinstance(i, dict)])
            rendered_inputs = dx.merge_dicts(default_inputs, self.inputs, True)

            # Calculate and set the initial workflow context.
            rendered_vars = expr.evaluate(getattr(self.spec, 'vars', {}), rendered_inputs)
            ctx_value = dx.merge_dicts(rendered_inputs, rendered_vars)
            self._flow.contexts.append({'srcs': [], 'value': ctx_value})

            # Identify the starting tasks and set the pointer to the initial context entry.
            for task_node in self.graph.roots:
                self._flow.staged[task_node['id']] = {'ctxs': [0]}

        return self._flow

    @property
    def inputs(self):
        return self._inputs

    def set_workflow_state(self, value):
        if not states.is_valid(value):
            raise exc.InvalidState(value)

        if not states.is_transition_valid(self.state, value):
            raise exc.InvalidStateTransition(self.state, value)

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
        if self.state not in states.COMPLETED_STATES:
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
            term_ctx_val = dx.merge_dicts(term_ctx_entry['value'], ctx_diff, True)
            term_ctx_entry['src'].append(task_flow_idx)
            term_ctx_entry['value'] = term_ctx_val

    def get_start_tasks(self):
        if self.state not in states.RUNNING_STATES:
            return []

        ctx_entry = self.get_workflow_initial_context()

        tasks = [
            {'id': node['id'], 'name': node['name'], 'ctx': ctx_entry['value']}
            for node in self.graph.roots
        ]

        return sorted(tasks, key=lambda x: x['name'])

    def get_next_tasks(self, task_id):
        if self.state not in states.RUNNING_STATES:
            return []

        task_flow_entry = self.get_task_flow_entry(task_id)

        if not task_flow_entry or task_flow_entry.get('state') not in states.COMPLETED_STATES:
            return []

        next_tasks = []
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
            next_task_id = next_task_node['id']
            next_task_name = next_task_node['name']
            next_task_ctx = self.get_task_initial_context(next_task_id)['value']
            next_task = {'id': next_task_id, 'name': next_task_name, 'ctx': next_task_ctx}
            next_tasks.append(next_task)

        return sorted(next_tasks, key=lambda x: x['name'])

    def get_task_flow_idx(self, task_id):
        return self.flow.tasks.get(task_id)

    def get_task_flow_entry(self, task_id):
        flow_idx = self.get_task_flow_idx(task_id)

        return self.flow.sequence[flow_idx] if flow_idx is not None else None

    def add_task_flow_entry(self, task_id, in_ctx_idx=None):
        if not self.graph.has_task(task_id):
            raise exc.InvalidTask(task_id)

        task_flow_entry = {'id': task_id, 'ctx': in_ctx_idx}
        self.flow.sequence.append(task_flow_entry)
        self.flow.tasks[task_id] = len(self.flow.sequence) - 1

        return task_flow_entry

    def update_task_flow_entry(self, task_id, state, result=None):
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
            task_flow_entry = self.add_task_flow_entry(task_id, in_ctx_idx=in_ctx_idx)

        # If task is alstaged completed and in cycle, then create new task flow entry.
        if self.graph.in_cycle(task_id) and task_flow_entry.get('state') in states.COMPLETED_STATES:
            task_flow_entry = self.add_task_flow_entry(task_id, in_ctx_idx=in_ctx_idx)

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
                criteria = task_transition[3].get('criteria') or []
                evaluated_criteria = [expr.evaluate(c, current_ctx) for c in criteria]
                task_transition_id = task_transition[1] + '__' + str(task_transition[2])
                task_flow_entry[task_transition_id] = all(evaluated_criteria)

                # If criteria met, then mark the next task staged and alculate outgoing context.
                if task_flow_entry[task_transition_id]:
                    next_task_node = self.graph.get_task(task_transition[1])
                    next_task_name = next_task_node['name']

                    out_ctx_val = task_spec.finalize_context(
                        next_task_name,
                        criteria,
                        copy.deepcopy(current_ctx)
                    )

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
        state = self.state

        if task_flow_entry['state'] == states.RESUMING:
            state = states.RESUMING
        elif task_flow_entry['state'] in states.RUNNING_STATES:
            state = states.RUNNING
        elif task_flow_entry['state'] in states.PAUSE_STATES and self.state == states.CANCELING:
            state = states.CANCELING if any_active_tasks else states.CANCELED
        elif task_flow_entry['state'] in states.PAUSE_STATES:
            state = states.PAUSING if any_active_tasks else states.PAUSED
        elif task_flow_entry['state'] in states.CANCEL_STATES:
            state = states.CANCELING if any_active_tasks else states.CANCELED
        elif task_flow_entry['state'] in states.COMPLETED_STATES and self.state == states.PAUSING:
            state = states.PAUSING if any_active_tasks else states.PAUSED
        elif task_flow_entry['state'] in states.COMPLETED_STATES and self.state == states.CANCELING:
            state = states.CANCELING if any_active_tasks else states.CANCELED
        elif task_flow_entry['state'] in states.ABENDED_STATES:
            state = states.RUNNING if any_next_tasks else states.FAILED
        elif task_flow_entry['state'] == states.SUCCEEDED:
            is_wf_running = any_active_tasks or any_next_tasks or any_staged_tasks
            state = states.RUNNING if is_wf_running else states.SUCCEEDED

        if self.state != state and states.is_transition_valid(self.state, state):
            self.set_workflow_state(state)

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
