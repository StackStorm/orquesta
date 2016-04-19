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
import re

import yaql


LOG = logging.getLogger(__name__)

YAQL_ENGINE = yaql.language.factory.YaqlFactory().create()
YAQL_ROOT_CONTEXT = yaql.create_context()


def strip_delimiter(expr):
    match = re.search('<%(.+?)%>', expr)

    if match:
        found = match.group(1)
        return found.strip()

    return expr


def evaluate(expr, data=None):
    ctx = YAQL_ROOT_CONTEXT.create_child_context()

    if isinstance(data, dict):
        ctx['__tasks'] = data.get('__tasks')
        ctx['$'] = data

    return YAQL_ENGINE(expr).evaluate(context=ctx)


def task_(context, task_name):
    return context['__tasks'][task_name]


def _register_custom_functions():
    YAQL_ROOT_CONTEXT.register_function(task_)


_register_custom_functions()
