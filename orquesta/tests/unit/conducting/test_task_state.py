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

import unittest

from orquesta import conducting
from orquesta.specs import native as native_specs
from orquesta.specs.native.v1 import models as native_v1_models


class TaskStateTest(unittest.TestCase):

    def test_task_state(self):
        wf_def = """
        version: 1.0

        tasks:
          task1:
            action: core.noop
            retry:
              when: '<% failed() %>'
              count: 5
              delay: 10
        """

        # Initialize workflow spec and conductor
        spec = native_specs.WorkflowSpec(wf_def)
        conductor = conducting.WorkflowConductor(spec)

        # Create task_state_entry manually
        task_state_entry = conducting.TaskState(conductor, **{
            'id': 'task1',
            'route': 0,
            'ctxs': {
                'in': [0],
            },
            'prev': {},
            'next': {}
        })

        self.assertIsInstance(task_state_entry.task_spec, native_v1_models.TaskSpec)
        self.assertEqual(task_state_entry.retry_count, 5)
        self.assertFalse(task_state_entry.will_retry())
        self.assertFalse(task_state_entry.is_retried())

    def test_task_state_without_conductor(self):
        # Create task_state_entry manually without conductor
        task_state_entry = conducting.TaskState(conductor=None, **{
            'id': 'task1',
            'route': 0,
            'ctxs': {
                'in': [0],
            },
            'prev': {},
            'next': {}
        })

        self.assertIsNone(task_state_entry.task_spec)
        self.assertEqual(task_state_entry.retry_count, 0)
        self.assertFalse(task_state_entry.will_retry())
        self.assertFalse(task_state_entry.is_retried())
