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
        self.ready = list()

    def serialize(self):
        return {
            'tasks': copy.deepcopy(self.tasks),
            'sequence': copy.deepcopy(self.sequence),
            'ready': copy.deepcopy(self.ready)
        }

    @classmethod
    def deserialize(cls, data):
        instance = cls()
        instance.tasks = copy.deepcopy(data.get('tasks', dict()))
        instance.sequence = copy.deepcopy(data.get('sequence', list()))
        instance.ready = copy.deepcopy(data.get('ready', list()))

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
        self._context = None

    def restore(self, graph, state=None, flow=None, inputs=None, context=None):
        if not graph or not isinstance(graph, graphing.WorkflowGraph):
            raise ValueError('The value of "graph" is not type of WorkflowGraph.')

        if not flow or not isinstance(flow, TaskFlow):
            raise ValueError('The value of "flow" is not type of TaskFlow.')

        if state and not states.is_valid(state):
            raise exc.InvalidState(state)

        if inputs is not None and not isinstance(inputs, dict):
            raise ValueError('The value of "inputs" is not type of dict.')

        if context is not None and not isinstance(context, dict):
            raise ValueError('The value of "context" is not type of dict.')

        self._workflow_state = state
        self._graph = graph
        self._flow = flow
        self._inputs = inputs or {}
        self._context = context or {}

    def serialize(self):
        return {
            'spec': self.spec.serialize(),
            'graph': self.graph.serialize(),
            'state': self.state,
            'flow': self.flow.serialize(),
            'inputs': copy.deepcopy(self.inputs),
            'context': copy.deepcopy(self.context)
        }

    @classmethod
    def deserialize(cls, data):
        spec_module = specs_loader.get_spec_module(data['spec']['catalog'])
        spec = spec_module.WorkflowSpec.deserialize(data['spec'])

        graph = graphing.WorkflowGraph.deserialize(data['graph'])
        state = data['state']
        flow = TaskFlow.deserialize(data['flow'])
        inputs = copy.deepcopy(data['inputs'])
        context = copy.deepcopy(data['context'])

        instance = cls(spec)
        instance.restore(graph, state, flow, inputs, context)

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

        return self._flow

    @property
    def inputs(self):
        return self._inputs

    @property
    def context(self):
        if self._context is not None:
            return self._context

        self._context = {}

        if self.inputs:
            rendered_vars = expr.evaluate(getattr(self.spec, 'vars', {}), self.inputs)
            self._context = dx.merge_dicts(rendered_vars, self.inputs)

        return self._context

    def set_workflow_state(self, value):
        if not states.is_valid(value):
            raise exc.InvalidState(value)

        if not states.is_transition_valid(self.state, value):
            raise exc.InvalidStateTransition(self.state, value)

        self._workflow_state = value

    def get_start_tasks(self):
        if self.state not in states.RUNNING_STATES:
            return []

        return self.graph.roots

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
                        satisfied.append(prev_task_flow_entry.get(prev_task_transition_id))

                if len(satisfied) < barrier:
                    continue

            next_task = self.graph.get_task(next_task_id)
            next_tasks.append({'id': next_task_id, 'name': next_task['name']})

        return sorted(next_tasks, key=lambda x: x['name'])

    def get_task_flow_idx(self, task_id):
        return self.flow.tasks.get(task_id)

    def get_task_flow_entry(self, task_id):
        flow_idx = self.get_task_flow_idx(task_id)

        return self.flow.sequence[flow_idx] if flow_idx is not None else None

    def add_task_flow_entry(self, task_id):
        if not self.graph.has_task(task_id):
            raise exc.InvalidTask(task_id)

        task_flow_entry = {'id': task_id}
        self.flow.sequence.append(task_flow_entry)
        self.flow.tasks[task_id] = len(self.flow.sequence) - 1

        return task_flow_entry

    def update_task_flow_entry(self, task_id, state, context=None):
        if not states.is_valid(state):
            raise exc.InvalidState(state)

        # Get task flow entry and create new if it does not exist.
        task_flow_entry = self.get_task_flow_entry(task_id)

        if not task_flow_entry:
            task_flow_entry = self.add_task_flow_entry(task_id)

        # If task is already completed and in cycle, then create new task flow entry.
        if self.graph.in_cycle(task_id) and task_flow_entry.get('state') in states.COMPLETED_STATES:
            task_flow_entry = self.add_task_flow_entry(task_id)

        # Check if the task state change is valid.
        if not states.is_transition_valid(task_flow_entry.get('state'), state):
            raise exc.InvalidStateTransition(task_flow_entry.get('state'), state)

        task_flow_entry['state'] = state

        # Remove the task from the ready list if it becomes active.
        if state in states.ACTIVE_STATES and task_id in self.flow.ready:
            self.flow.ready.remove(task_id)

        # Set current task in the context.
        task_node = self.graph.get_task(task_id)
        current_task = {'id': task_node['id'], 'name': task_node['name']}
        context = ctx.set_current_task(context or {}, current_task)

        # Evaluate task transitions if task is in completed state.
        if state in states.COMPLETED_STATES:
            # Setup context for evaluating expressions in task transition criteria.
            context = dx.merge_dicts(context, {'__flow': self.flow.serialize()}, True)

            # Iterate thru each outbound task transitions.
            for t in self.graph.get_next_transitions(task_id):
                criteria = t[3].get('criteria') or []
                evaluated_criteria = [expr.evaluate(c, context) for c in criteria]
                task_transition_id = t[1] + '__' + str(t[2])
                task_flow_entry[task_transition_id] = all(evaluated_criteria)
                if task_flow_entry[task_transition_id]:
                    self.flow.ready.append(t[1])

        # Identify if there are task transitions.
        any_next_tasks = False

        for t in self.graph.get_next_transitions(task_id):
            task_transition_id = t[1] + '__' + str(t[2])
            any_next_tasks = task_flow_entry.get(task_transition_id, False)
            if any_next_tasks:
                break

        # Identify if there are any other active tasks.
        any_active_tasks = any([t['state'] in states.ACTIVE_STATES for t in self.flow.sequence])

        # Identify if there are any other ready tasks.
        any_ready_tasks = len(self.flow.ready) > 0

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
            is_wf_running = any_active_tasks or any_next_tasks or any_ready_tasks
            state = states.RUNNING if is_wf_running else states.SUCCEEDED

        if self.state != state and states.is_transition_valid(self.state, state):
            self.set_workflow_state(state)

        return task_flow_entry
