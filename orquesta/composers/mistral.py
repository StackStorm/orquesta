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
from six.moves import queue

from orquesta.composers import base
from orquesta import graphing
from orquesta.specs import mistral as specs
from orquesta import states


LOG = logging.getLogger(__name__)

TASK_TRANSITION_MAP = {
    'on-success': [states.SUCCEEDED],
    'on-error': states.ABENDED_STATES,
    'on-complete': states.COMPLETED_STATES
}


class WorkflowComposer(base.WorkflowComposer):
    wf_spec_type = specs.WorkflowSpec

    @classmethod
    def compose(cls, spec):
        if not cls.wf_spec_type:
            raise TypeError('Undefined spec type for composer.')

        if not isinstance(spec, cls.wf_spec_type):
            raise TypeError('Unsupported spec type "%s".' % str(type(spec)))

        wf_graph = cls._compose_wf_graph(spec)

        return cls._compose_wf_ex_graph(wf_graph)

    @classmethod
    def _compose_transition_criteria(cls, task_name, *args, **kwargs):
        criteria = []

        condition = kwargs.get('condition')
        expr = kwargs.get('expr')

        task_state_criterion = (
            'task_state(%s) in %s' % (task_name, str(TASK_TRANSITION_MAP[condition]))
        )

        criteria.append('<% ' + task_state_criterion + ' %>')

        if expr:
            criteria.append(expr)

        return criteria

    @classmethod
    def _compose_wf_graph(cls, wf_spec):
        if not isinstance(wf_spec, cls.wf_spec_type):
            raise TypeError('Workflow spec is not typeof %s.' % cls.wf_spec_type.__name__)

        q = queue.Queue()
        wf_graph = graphing.WorkflowGraph()

        for task_name, expr, condition in wf_spec.tasks.get_start_tasks():
            q.put((task_name, []))

        while not q.empty():
            task_name, splits = q.get()

            wf_graph.add_task(task_name)

            if wf_spec.tasks.is_join_task(task_name):
                task_spec = wf_spec.tasks[task_name]
                barrier = '*' if task_spec.join == 'all' else task_spec.join
                wf_graph.set_barrier(task_name, value=barrier)

            # Determine if the task is a split task and if it is in a cycle. If the task is a
            # split task, keep track of where the split(s) occurs.
            if wf_spec.tasks.is_split_task(task_name) and not wf_spec.tasks.in_cycle(task_name):
                splits.append(task_name)

            if splits:
                wf_graph.update_task(task_name, splits=splits)

            next_tasks = wf_spec.tasks.get_next_tasks(task_name)

            for next_task_name, expr, condition in next_tasks:
                if (not wf_graph.has_task(next_task_name) or
                        not wf_spec.tasks.in_cycle(next_task_name)):
                    q.put((next_task_name, list(splits)))

                crta = cls._compose_transition_criteria(task_name, condition=condition, expr=expr)
                seqs = wf_graph.has_transition(task_name, next_task_name, criteria=crta)

                # Use existing transition if present otherwise create new transition.
                if seqs:
                    wf_graph.update_transition(
                        task_name,
                        next_task_name,
                        key=seqs[0][2],
                        criteria=crta
                    )
                else:
                    wf_graph.add_transition(
                        task_name,
                        next_task_name,
                        criteria=crta
                    )

        return wf_graph

    @classmethod
    def _compose_wf_ex_graph(cls, wf_graph):
        q = queue.Queue()
        split_counter = {}
        wf_ex_graph = graphing.WorkflowGraph()

        def _create_task_ex_name(task_name, split_id):
            return task_name + '__' + str(split_id) if split_id > 0 else task_name

        for task in wf_graph.roots:
            q.put((task['id'], None, None, []))

        while not q.empty():
            task_name, prev_task_ex_name, criteria, splits = q.get()
            task_ex_attrs = wf_graph.get_task(task_name)
            task_ex_attrs['name'] = task_name

            # For complex multi-level splits and joins, if a task from higher in the hierarchy is
            # processed first, then ignore the task for now. This task will be processed again
            # later in the hierarchy. Otherwise, if this task is processed now, it will be placed
            # in a separate workflow branch.
            expected_splits = task_ex_attrs.pop('splits', [])
            prev_task_ex = wf_ex_graph.get_task(prev_task_ex_name) if prev_task_ex_name else {}

            if (expected_splits and
                    task_name not in expected_splits and
                    not prev_task_ex.get('splits', [])):
                continue

            # Determine if the task is a split task and if it is in a cycle. If the task is a split
            # task, keep track of how many instances and which branch the instance belongs to.
            is_split_task = (
                len(wf_graph.get_prev_transitions(task_name)) > 1 and
                not wf_graph.has_barrier(task_name)
            )

            is_task_in_cycle = wf_graph.in_cycle(task_name)

            if is_split_task and not is_task_in_cycle:
                split_counter[task_name] = split_counter.get(task_name, 0) + 1
                splits.append((task_name, split_counter[task_name]))

            if splits:
                task_ex_attrs['splits'] = splits

            task_ex_name = _create_task_ex_name(task_name, splits[-1][1] if splits else 0)

            # If the task already exists in the execution graph, the task is already processed
            # and this is a cycle in the graph.
            if wf_ex_graph.has_task(task_ex_name):
                wf_ex_graph.update_task(task_ex_name, **task_ex_attrs)
            else:
                wf_ex_graph.add_task(task_ex_name, **task_ex_attrs)

                for next_seq in wf_graph.get_next_transitions(task_name):
                    next_seq_criteria = [
                        c.replace('task_state(%s)' % task_name, 'task_state(%s)' % task_ex_name)
                        for c in next_seq[3]['criteria']
                    ]

                    q.put((next_seq[1], task_ex_name, next_seq_criteria, list(splits)))

            # A split task should only have one previous transition even if there are multiple
            # different tasks transitioning to it. Since it has no join requirement, the split
            # task will create a new instance and execute.
            if is_split_task and prev_task_ex_name:
                wf_ex_graph.add_transition(prev_task_ex_name, task_ex_name, criteria=criteria)
                continue

            # Finally, process all inbound task transitions.
            for prev_seq in wf_graph.get_prev_transitions(task_name):
                prev_task = wf_graph.get_task(prev_seq[0])
                split_id = 0

                for prev_task_split in prev_task.get('splits', []):
                    matches = [s for s in splits if s[0] == prev_task_split]
                    split_id = matches[0][1] if matches else split_id

                p_task_name = prev_seq[0]
                p_task_ex_name = _create_task_ex_name(p_task_name, split_id)

                p_seq_criteria = [
                    c.replace('task_state(%s)' % p_task_name, 'task_state(%s)' % p_task_ex_name)
                    for c in prev_seq[3]['criteria']
                ]

                seqs = wf_ex_graph.has_transition(
                    p_task_ex_name,
                    task_ex_name,
                    criteria=p_seq_criteria
                )

                # Use existing transition if present otherwise create new transition.
                if seqs:
                    wf_ex_graph.update_transition(
                        p_task_ex_name,
                        task_ex_name,
                        key=seqs[0][2],
                        criteria=p_seq_criteria
                    )
                else:
                    wf_ex_graph.add_transition(
                        p_task_ex_name,
                        task_ex_name,
                        criteria=p_seq_criteria
                    )

        return wf_ex_graph
