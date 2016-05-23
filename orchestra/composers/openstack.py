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
import uuid

from orchestra.composers import base
from orchestra import composition
from orchestra import specs
from orchestra import states
from orchestra.utils import expression


LOG = logging.getLogger(__name__)


class MistralWorkflowComposer(base.WorkflowComposer):

    @staticmethod
    def _compose_sequence_criteria(task_name, task_state, expr=None):
        yaql_expr = (
            'task(%s).get(state, "%s") = "%s"' % (
                task_name,
                states.UNKNOWN,
                task_state
            )
        )

        if expr:
            yaql_expr += ' and (%s)' % expression.strip_delimiter(expr)

        return {'yaql': yaql_expr}

    @classmethod
    def _compose_wf_graph(cls, wf_spec):
        q = queue.Queue()
        wf_graph = composition.WorkflowGraph()

        for task_name in wf_spec.get_start_tasks():
            q.put((task_name, []))

        while not q.empty():
            task_name, splits = q.get()

            wf_graph.add_task(task_name)

            if wf_spec.is_join_task(task_name):
                task_spec = wf_spec.get_task(task_name)
                wf_graph.update_task(task_name, join=task_spec['join'])

            # Determine if the task is a split task and if it is in a cycle.
            # If the task is a split task, keep track of where the split(s)
            # occurs.
            if (wf_spec.is_split_task(task_name) and
                    not wf_spec.in_cycle(task_name)):
                splits.append(task_name)

            if splits:
                wf_graph.update_task(task_name, splits=splits)

            next_tasks = wf_spec.get_next_tasks(task_name)

            for next_task_name, expr, condition in next_tasks:
                if (not wf_graph.has_task(next_task_name) or
                        not wf_spec.in_cycle(next_task_name)):
                    q.put((next_task_name, list(splits)))

                if not wf_graph.has_sequence(task_name, next_task_name):
                    criteria = cls._compose_sequence_criteria(
                        task_name,
                        states.SUCCESS,
                        expr=expr
                    )

                    wf_graph.add_sequence(
                        task_name,
                        next_task_name,
                        criteria=criteria
                    )

        return wf_graph

    @classmethod
    def _compose_wf_ex_graph(cls, wf_graph):
        q = queue.Queue()
        split_counter = {}
        wf_ex_graph = composition.WorkflowGraph()
        nodes = dict(wf_graph._graph.nodes(data=True))

        def create_task_ex_name(task_name, split_id):
            return (
                task_name + '__' + str(split_id)
                if split_id > 0
                else task_name
            )

        for task in wf_graph.get_start_tasks():
            q.put((task['id'], None, []))

        while not q.empty():
            task_name, prev_task_ex_name, splits = q.get()
            attributes = copy.deepcopy(nodes[task_name])
            attributes['name'] = task_name

            # For complex multi-level splits and joins, if a task from higher
            # in the hierarchy is processed first, the task should be ignored.
            # The task will be processed again later in the hierarchy.
            # Otherwise, the task will be treated as a separate branch path.
            expected_splits = attributes.pop('splits', [])
            prev_task = (
                wf_ex_graph.get_task(prev_task_ex_name)
                if prev_task_ex_name else {}
            )

            if (expected_splits and
                    task_name not in expected_splits and
                    not prev_task.get('splits', {})):
                continue

            # Determine if the task is a split task and if it is in a cycle.
            # If the task is a split task, keep track of how many instances
            # and which branch the instance belows to.
            is_split_task = wf_graph.is_split_task(task_name)
            is_task_in_cycle = wf_graph.in_cycle(task_name)

            if is_split_task and not is_task_in_cycle:
                split_id = split_counter.get(task_name, 0) + 1
                split_counter[task_name] = split_id
                split = (task_name, split_id)
                splits.append(split)

            if splits:
                attributes['splits'] = splits

            task_ex_name = create_task_ex_name(
                task_name,
                splits[-1][1] if splits else 0
            )

            # If the task already exists in the execution graph, the task is
            # already processed and this is a cycle in the graph. 
            if wf_ex_graph.has_task(task_ex_name):
                wf_ex_graph.update_task(task_ex_name, **attributes)
            else:
                wf_ex_graph.add_task(task_ex_name, **attributes)

                for next_seq in wf_graph.get_next_sequences(task_name):
                    q.put((next_seq[1], task_ex_name, list(splits)))

            # A split task should only have one previous sequence even if there
            # are multiple different tasks transitioning to it. Since it has
            # no join requirement, the split task will create a new instance
            # and execute.
            if is_split_task and prev_task:
                seq = wf_graph.get_sequence(prev_task['name'], task_name)

                wf_ex_graph.add_sequence(
                    prev_task_ex_name,
                    task_ex_name,
                    **seq[2]
                )

                continue

            # Finally, process all inbound task transitions.
            for seq in wf_graph.get_prev_sequences(task_name):
                prev_task = wf_graph.get_task(seq[0])

                split_srcs = [item[0] for item in splits]
                matches = [
                    split_src for split_src in prev_task.get('splits', [])
                    if split_src in split_srcs
                ]

                prev_task_ex_name = create_task_ex_name(
                    seq[0],
                    splits[-1][1] if matches else 0
                )

                wf_ex_graph.add_sequence(
                    prev_task_ex_name,
                    task_ex_name,
                    **seq[2]
                )

        return wf_ex_graph

    @classmethod
    def compose(cls, definition):
        wf_spec = specs.WorkflowSpec(definition)

        wf_graph = cls._compose_wf_graph(wf_spec)

        return cls._compose_wf_ex_graph(wf_graph)
