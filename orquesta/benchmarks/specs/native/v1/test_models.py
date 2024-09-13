# Copyright 2021 The StackStorm Authors.
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

import pytest

import orquesta.specs.native.v1.models as models

from orquesta.expressions import base as expr_base
from orquesta.utils import context as ctx_util


WITH_ITEMS = [
    [{"test": "s"}, {"test1": "s"}, {"test2": "s"}],
    [{"test": "s" * 10000}, {"test1": "s" * 10000}, {"test2": "s" * 10000}],
    [{"test": "s" * 1000000}, {"test1": "s" * 1000000}, {"test2": "s" * 1000000}],
]


@pytest.mark.parametrize("fixture", WITH_ITEMS, ids=["small", "medium", "large"])
@pytest.mark.benchmark(group="no deepcopy")
def test_task_spec_render(benchmark, fixture):
    def run_benchmark():
        # Instantiate workflow spec.
        task_spec = models.TaskSpec(
            {
                "action": "core.echo message=<% item() %>",
                "next": [{"publish": [{"items": "<% result() %>"}]}],
                "with": {"items": "<% ctx(xs) %>"},
            }
        )
        in_ctx = {
            "xs": fixture,
            "__current_task": {"id": "task1", "route": 0},
            "__state": {
                "contexts": [{"xs": fixture}],
                "routes": [[]],
                "sequence": [],
                "staged": [
                    {"id": "task1", "ctxs": {"in": [0]}, "route": 0, "prev": {}, "ready": True}
                ],
                "status": "running",
                "tasks": {},
            },
        }
        # Instantiate conductor
        task_spec.render(in_ctx)

    benchmark(run_benchmark)


class OldTaskSpec(models.TaskSpec):
    def render(self, in_ctx):
        action_specs = []

        if not self.has_items():
            action_spec = {
                "action": expr_base.evaluate(self.action, in_ctx),
                "input": expr_base.evaluate(getattr(self, "input", {}), in_ctx),
            }

            action_specs.append(action_spec)
        else:
            items_spec = self.get_items_spec()

            if " in " not in items_spec.items:
                items_expr = items_spec.items.strip()
            else:
                start_idx = items_spec.items.index(" in ") + 4
                items_expr = items_spec.items[start_idx:].strip()

            items = expr_base.evaluate(items_expr, in_ctx)

            if not isinstance(items, list):
                raise TypeError('The value of "%s" is not type of list.' % items_expr)

            item_keys = (
                None
                if " in " not in items_spec.items
                else items_spec.items[: items_spec.items.index(" in ")].replace(" ", "").split(",")
            )

            for idx, item in enumerate(items):
                if item_keys and (isinstance(item, tuple) or isinstance(item, list)):
                    item = dict(zip(item_keys, list(item)))
                elif item_keys and len(item_keys) == 1:
                    item = {item_keys[0]: item}

                item_ctx_value = ctx_util.set_current_item(in_ctx, item)

                action_spec = {
                    "action": expr_base.evaluate(self.action, item_ctx_value),
                    "input": expr_base.evaluate(getattr(self, "input", {}), item_ctx_value),
                    "item_id": idx,
                }

                action_specs.append(action_spec)

        return self, action_specs


@pytest.mark.parametrize("fixture", WITH_ITEMS, ids=["small", "medium", "large"])
@pytest.mark.benchmark(group="deepcopy")
def test_task_spec_render_old(benchmark, fixture):
    def run_benchmark():
        # Instantiate workflow spec.
        task_spec = OldTaskSpec(
            {
                "action": "core.echo message=<% item() %>",
                "next": [{"publish": [{"items": "<% result() %>"}]}],
                "with": {"items": "<% ctx(xs) %>"},
            }
        )
        in_ctx = {
            "xs": fixture,
            "__current_task": {"id": "task1", "route": 0},
            "__state": {
                "contexts": [{"xs": fixture}],
                "routes": [[]],
                "sequence": [],
                "staged": [
                    {"id": "task1", "ctxs": {"in": [0]}, "route": 0, "prev": {}, "ready": True}
                ],
                "status": "running",
                "tasks": {},
            },
        }
        # Instantiate conductor
        task_spec.render(in_ctx)

    benchmark(run_benchmark)
