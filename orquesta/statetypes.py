# Copyright 2025 The StackStorm Authors.
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

"""Static type definitions for the runtime state passed around the conductor.

The workflow conductor (:mod:`orquesta.conducting`) and the state machines
(:mod:`orquesta.machines`) exchange a handful of plain ``dict`` structures that
are serialized to JSON and persisted (e.g. in StackStorm's database). Because
they are plain dicts, their shape is invisible to readers, IDEs, and type
checkers -- you have to grep the code to learn what keys exist.

These ``TypedDict`` definitions document those shapes *without changing any
runtime behavior*: a ``TypedDict`` is an ordinary ``dict`` at run time, so
``serialize``/``deserialize`` and the on-disk/wire format are completely
unaffected. They exist purely so method signatures can say what they accept and
return, and so ``mypy`` / editors can check key access.

Compatibility note: orquesta targets Python 3.10+, where ``typing.NotRequired``
is not yet available (3.11+). Optional keys are therefore expressed with the
"required base class + ``total=False`` subclass" idiom, and the functional
``TypedDict(...)`` form is used where a key name is a Python keyword (``in``).
"""

from __future__ import annotations

from typing import Any
from typing import Dict
from typing import List
from typing import TypedDict


# ---------------------------------------------------------------------------
# Shared leaf structures
# ---------------------------------------------------------------------------

# A context "pointer" bundle. "in" is a list of indexes into
# ``WorkflowState.contexts``; "out" maps a task-transition id to the index of
# the context produced for that transition. Note "in" is a Python keyword, so
# the functional TypedDict syntax is required here.
_TaskContextsIn = TypedDict("_TaskContextsIn", {"in": List[int]})
_TaskContextsOut = TypedDict("_TaskContextsOut", {"out": Dict[str, int]}, total=False)


class TaskContexts(_TaskContextsIn, _TaskContextsOut):
    """``{"in": [<ctx idx>, ...], "out"?: {<transition id>: <ctx idx>}}``.

    ``in`` is always present; ``out`` is only added once a task completes and
    produces a new context for one of its outbound transitions.
    """


class ItemState(TypedDict):
    """Per-item execution status for a with-items task (``staged["items"][i]``)."""

    status: str


class _RetryStateRequired(TypedDict):
    # Comes from the task's retry spec plus a runtime ``tally`` counter.
    when: Any  # spec expression string, or None when unconditional
    count: int  # max number of retries
    tally: int  # number of retries performed so far


class RetryState(_RetryStateRequired, total=False):
    """Runtime retry bookkeeping stored under a task entry's ``retry`` key."""

    delay: int  # resolved delay (seconds) between retries


# ---------------------------------------------------------------------------
# Task state entry (an element of ``WorkflowState.sequence``)
# ---------------------------------------------------------------------------


class _TaskStateEntryRequired(TypedDict):
    id: str
    route: int
    ctxs: TaskContexts
    # backref transition id -> index of the predecessor entry in ``sequence``.
    prev: Dict[str, int]
    # outbound transition id -> whether its criteria evaluated to True.
    next: Dict[str, bool]


class TaskStateEntry(_TaskStateEntryRequired, total=False):
    """A task execution record in ``WorkflowState.sequence``.

    ``status`` is absent until the task state machine processes its first
    event; the remaining keys are set only in specific situations (retry
    configured, task is terminal, task marked to be ignored on rerun).
    """

    status: str
    retry: RetryState
    term: bool
    ignore: bool


# ---------------------------------------------------------------------------
# Staged task (an element of ``WorkflowState.staged``)
# ---------------------------------------------------------------------------


class _StagedTaskRequired(TypedDict):
    id: str
    route: int
    ctxs: TaskContexts
    prev: Dict[str, int]
    ready: bool


class StagedTask(_StagedTaskRequired, total=False):
    """A task queued to (potentially) run next, in ``WorkflowState.staged``.

    ``items``/``completed`` appear for with-items tasks, ``retry`` when the
    task is re-staged for a retry, and ``run_on_fail`` when the task should
    still run after the workflow has failed (remediation).
    """

    items: List[ItemState]
    completed: bool
    retry: RetryState
    run_on_fail: bool


# ---------------------------------------------------------------------------
# Serialized workflow state (WorkflowState.serialize / deserialize)
# ---------------------------------------------------------------------------

# "task__rN" -> index into ``sequence`` of that task/route's latest entry.
TaskIndex = Dict[str, int]
# Each route is the ordered list of split transition ids that define it.
RouteDetails = List[str]


class _SerializedWorkflowStateRequired(TypedDict):
    contexts: List[Dict[str, Any]]
    routes: List[RouteDetails]
    sequence: List[TaskStateEntry]
    staged: List[StagedTask]
    status: str
    tasks: TaskIndex


class SerializedWorkflowState(_SerializedWorkflowStateRequired, total=False):
    """The JSON-serializable form produced by ``WorkflowState.serialize``.

    ``reruns`` is only included when at least one rerun has been requested.
    """

    # Each entry is the list of ``sequence`` indexes rerun in one rerun request.
    reruns: List[List[int]]


# ---------------------------------------------------------------------------
# Transient (non-serialized) structures
# ---------------------------------------------------------------------------


class _RuntimeTaskRequired(TypedDict):
    id: str
    route: int
    ctx: Dict[str, Any]
    spec: Any  # a rendered TaskSpec instance (orquesta.specs.*)
    actions: List[Any]  # list of rendered action specs


class RuntimeTask(_RuntimeTaskRequired, total=False):
    """The in-memory "next task" dict built by ``WorkflowConductor.get_task``.

    This is *not* serialized -- it is handed to the caller (e.g. the workflow
    engine) to describe a task to schedule. ``delay`` is added when the task
    defines one; ``items_count``/``concurrency`` are added for with-items tasks.
    """

    delay: int
    items_count: int
    concurrency: Any  # resolved concurrency (int) or None


class _LogEntryRequired(TypedDict):
    type: str  # "info" | "warn" | "error"
    message: str


class LogEntry(_LogEntryRequired, total=False):
    """An entry appended to the conductor's ``log`` or ``errors`` lists.

    The optional keys are only present when the corresponding argument was
    provided to ``WorkflowConductor.log_entry`` (nulls are not inserted).
    """

    task_id: str
    route: int
    task_transition_id: str
    result: Any
    data: Any
