# Copyright 2019 The Linux Foundation.
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

from orquesta.specs.mistral.v2 import policies as policy_models
from orquesta.specs.mistral.v2 import tasks as task_models
from orquesta.tests.unit.specs.mistral import base as test_base


class TaskSpecTest(test_base.MistralWorkflowSpecTest):

    def test_simplest_task(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: The simplest workflow
                tasks:
                    task1:
                        action: std.noop
        """

        task_spec = self.instantiate(wf_def).tasks.get_task('task1')

        self.assertIsInstance(task_spec, task_models.TaskSpec)
        self.assertIsNone(task_spec.has_retry())

    def test_retry_task(self):
        wf_def = """
            version: '2.0'

            sequential:
                description: Workflow which has a retrying task
                tasks:
                    task1:
                        action: std.noop
                        retry:
                            delay: 5
                            count: 10
        """

        task_spec = self.instantiate(wf_def).tasks.get_task('task1')
        task_retry = task_spec.has_retry()

        self.assertIsInstance(task_retry, policy_models.RetrySpec)
        self.assertEqual(task_retry.delay, 5)
        self.assertEqual(task_retry.count, 10)
