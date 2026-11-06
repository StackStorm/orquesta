# Copyright 2026 P1naCol4da.
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

"""Regression tests for transitive task rerun request deduplication."""

import pytest
import yaml

from orquesta import conducting
from orquesta import events
from orquesta import requests
from orquesta.specs import native as native_specs
from orquesta import statuses


SHAPES = {
    "chain3": (
        ("task1", "task2", "task3"),
        (("task1", "task2"), ("task2", "task3")),
    ),
    "chain4": (
        ("task1", "task2", "task3", "task4"),
        (("task1", "task2"), ("task2", "task3"), ("task3", "task4")),
    ),
    "fork3": (
        ("task1", "task2", "task3"),
        (("task1", "task2"), ("task1", "task3")),
    ),
}


def _completed_conductor(shape):
    """Run a small real-conductor workflow to succeeded with synthetic events."""
    nodes, edges = SHAPES[shape]
    graph = {node: [] for node in nodes}
    for predecessor, successor in edges:
        graph[predecessor].append(successor)

    tasks = {}
    for node in nodes:
        task = {"action": "core.noop"}
        if graph[node]:
            next_steps = []
            for next_task in graph[node]:
                next_steps.append({"when": "<% succeeded() %>", "do": next_task})
            task["next"] = next_steps
        tasks[node] = task

    workflow_yaml = yaml.safe_dump({"version": 1.0, "tasks": tasks}, sort_keys=False)
    spec = native_specs.WorkflowSpec(workflow_yaml)
    assert spec.inspect() == {}

    conductor = conducting.WorkflowConductor(spec)
    conductor.request_workflow_status(statuses.RUNNING)
    seen = set()
    for _ in range(len(nodes) + 2):
        next_tasks = conductor.get_next_tasks()
        if not next_tasks:
            break
        for task in next_tasks:
            task_id, route = task["id"], task["route"]
            assert task_id not in seen, "Unexpected duplicate initial execution"
            seen.add(task_id)
            running_event = events.ActionExecutionEvent(statuses.RUNNING)
            conductor.update_task_state(task_id, route, running_event)
            success_event = events.ActionExecutionEvent(statuses.SUCCEEDED)
            conductor.update_task_state(task_id, route, success_event)

    assert seen == set(nodes)
    assert conductor.get_workflow_status() == statuses.SUCCEEDED
    return conductor


@pytest.mark.parametrize(
    "shape,requested,expected",
    [
        ("chain3", ("task1", "task3"), ("task1",)),
        ("chain3", ("task1", "task2", "task3"), ("task1",)),
        ("chain4", ("task1", "task4"), ("task1",)),
        ("fork3", ("task1", "task2", "task3"), ("task1",)),
        ("fork3", ("task2", "task3"), ("task2", "task3")),
        ("chain3", ("task3",), ("task3",)),
    ],
)
def test_rerun_request_collapses_transitive_descendants(shape, requested, expected):
    conductor = _completed_conductor(shape)
    rerun_requests = [requests.TaskRerunRequest.new(node, route=0) for node in requested]
    conductor.request_workflow_rerun(task_requests=rerun_requests)
    assert conductor.get_workflow_status() == statuses.RESUMING
    assert sorted(t["id"] for t in conductor.workflow_state.get_staged_tasks()) == sorted(expected)
    assert sorted(t["id"] for t in conductor.get_next_tasks()) == sorted(expected)
