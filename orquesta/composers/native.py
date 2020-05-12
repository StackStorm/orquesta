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
from six.moves import queue

from orquesta.composers import base as comp_base
from orquesta import graphing
from orquesta.specs import native as native_specs


LOG = logging.getLogger(__name__)


class WorkflowComposer(comp_base.WorkflowComposer):
    wf_spec_type = native_specs.WorkflowSpec

    @classmethod
    def compose(cls, spec):
        if not cls.wf_spec_type:
            raise TypeError("Undefined spec type for composer.")

        if not isinstance(spec, cls.wf_spec_type):
            raise TypeError('Unsupported spec type "%s".' % str(type(spec)))

        return cls._compose_wf_graph(spec)

    @classmethod
    def _compose_wf_graph(cls, wf_spec):
        if not isinstance(wf_spec, cls.wf_spec_type):
            raise TypeError("Workflow spec is not typeof %s." % cls.wf_spec_type.__name__)

        q = queue.Queue()
        wf_graph = graphing.WorkflowGraph()

        for task_name, condition, task_transition_item_idx in wf_spec.tasks.get_start_tasks():
            q.put((task_name, []))

        while not q.empty():
            task_name, splits = q.get()

            wf_graph.add_task(task_name)

            if wf_spec.tasks.is_join_task(task_name):
                task_spec = wf_spec.tasks[task_name]
                barrier = "*" if task_spec.join == "all" else task_spec.join
                wf_graph.set_barrier(task_name, value=barrier)

            # Determine if the task is a split task and if it is in a cycle. If the task is a
            # split task, keep track of where the split(s) occurs.
            if wf_spec.tasks.is_split_task(task_name) and not wf_spec.tasks.in_cycle(task_name):
                splits.append(task_name)

            if splits:
                wf_graph.update_task(task_name, splits=splits)

            # Update task attributes if task spec has retry criteria.
            task_spec = wf_spec.tasks.get_task(task_name)

            if task_spec.has_retry():
                retry_spec = {
                    "when": getattr(task_spec.retry, "when", None),
                    "count": getattr(task_spec.retry, "count", None),
                    "delay": getattr(task_spec.retry, "delay", None),
                }

                wf_graph.update_task(task_name, retry=retry_spec)

            # Add task transition to the workflow graph.
            next_tasks = wf_spec.tasks.get_next_tasks(task_name)

            for next_task_name, condition, task_transition_item_idx in next_tasks:
                if next_task_name == "retry":
                    retry_spec = {"when": condition or "<% completed() %>", "count": 3}
                    wf_graph.update_task(task_name, retry=retry_spec)
                    continue

                if not wf_graph.has_task(next_task_name) or not wf_spec.tasks.in_cycle(
                    next_task_name
                ):
                    q.put((next_task_name, list(splits)))

                crta = [condition] if condition else []

                seqs = wf_graph.has_transition(
                    task_name, next_task_name, criteria=crta, ref=task_transition_item_idx
                )

                # Use existing transition if present otherwise create new transition.
                if seqs:
                    wf_graph.update_transition(
                        task_name,
                        next_task_name,
                        key=seqs[0][2],
                        criteria=crta,
                        ref=task_transition_item_idx,
                    )
                else:
                    wf_graph.add_transition(
                        task_name, next_task_name, criteria=crta, ref=task_transition_item_idx
                    )

        return wf_graph
