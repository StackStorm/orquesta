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

import abc
import logging
import six
from six.moves import queue

from orchestra import composition
from orchestra.expressions import base as expressions
from orchestra.specs import v2 as specs
from orchestra.utils import plugin


LOG = logging.getLogger(__name__)


def get_composer(workflow_type):
    return plugin.get_module('orchestra.composers', workflow_type)


@six.add_metaclass(abc.ABCMeta)
class WorkflowComposer(object):
    wf_spec_cls = specs.DirectWorkflowSpec
    expr_evaluator = expressions.get_evaluator('yaql')

    @classmethod
    def compose(cls, definition):
        wf_spec = cls.wf_spec_cls(definition)

        wf_graph = cls._compose_wf_graph(wf_spec)

        return cls._compose_wf_ex_graph(wf_graph)

    @classmethod
    def _compose_sequence_criteria(cls, task_name, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def _compose_wf_graph(cls, wf_spec):
        raise NotImplementedError()

    @classmethod
    def _compose_wf_ex_graph(cls, wf_graph):
        q = queue.Queue()
        split_counter = {}
        wf_ex_graph = composition.WorkflowGraph()

        def _create_task_ex_name(task_name, split_id):
            return (
                task_name + '__' + str(split_id)
                if split_id > 0
                else task_name
            )

        for task in wf_graph.get_start_tasks():
            q.put((task['id'], None, None, []))

        while not q.empty():
            task_name, prev_task_ex_name, criteria, splits = q.get()
            task_ex_attrs = wf_graph.get_task(task_name)
            task_ex_attrs['name'] = task_name

            # For complex multi-level splits and joins, if a task from higher
            # in the hierarchy is processed first, then ignore the task for
            # now. This task will be processed again later in the hierarchy.
            # Otherwise, if this task is processed now, it will be placed in a
            # separate workflow branch.
            expected_splits = task_ex_attrs.pop('splits', [])
            prev_task_ex = (
                wf_ex_graph.get_task(prev_task_ex_name)
                if prev_task_ex_name else {}
            )

            if (expected_splits and
                    task_name not in expected_splits and
                    not prev_task_ex.get('splits', [])):
                continue

            # Determine if the task is a split task and if it is in a cycle.
            # If the task is a split task, keep track of how many instances
            # and which branch the instance belongs to.
            is_split_task = wf_graph.is_split_task(task_name)
            is_task_in_cycle = wf_graph.in_cycle(task_name)

            if is_split_task and not is_task_in_cycle:
                split_counter[task_name] = split_counter.get(task_name, 0) + 1
                splits.append((task_name, split_counter[task_name]))

            if splits:
                task_ex_attrs['splits'] = splits

            task_ex_name = _create_task_ex_name(
                task_name,
                splits[-1][1] if splits else 0
            )

            # If the task already exists in the execution graph, the task is
            # already processed and this is a cycle in the graph.
            if wf_ex_graph.has_task(task_ex_name):
                wf_ex_graph.update_task(task_ex_name, **task_ex_attrs)
            else:
                wf_ex_graph.add_task(task_ex_name, **task_ex_attrs)

                for next_seq in wf_graph.get_next_sequences(task_name):
                    item = (
                        next_seq[1],
                        task_ex_name,
                        next_seq[3]['criteria'],
                        list(splits)
                    )

                    q.put(item)

            # A split task should only have one previous sequence even if there
            # are multiple different tasks transitioning to it. Since it has
            # no join requirement, the split task will create a new instance
            # and execute.
            if is_split_task and prev_task_ex_name:
                wf_ex_graph.add_sequence(
                    prev_task_ex_name,
                    task_ex_name,
                    criteria=criteria
                )

                continue

            # Finally, process all inbound task transitions.
            for prev_seq in wf_graph.get_prev_sequences(task_name):
                prev_task = wf_graph.get_task(prev_seq[0])

                split_id = 0
                for prev_task_split in prev_task.get('splits', []):
                    matches = [s for s in splits if s[0] == prev_task_split]
                    split_id = matches[0][1] if matches else split_id

                wf_ex_graph.add_sequence(
                    _create_task_ex_name(prev_seq[0], split_id),
                    task_ex_name,
                    criteria=prev_seq[3]['criteria']
                )

        return wf_ex_graph
