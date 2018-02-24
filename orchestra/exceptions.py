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


LOG = logging.getLogger(__name__)


class PluginFactoryError(Exception):
    pass


class ExpressionGrammarException(Exception):
    pass


class ExpressionEvaluationException(Exception):
    pass


class SchemaDefinitionError(Exception):
    pass


class SchemaIncompatibleError(Exception):
    pass


class ContextValueError(Exception):
    pass


class InvalidTask(Exception):

    def __init__(self, task_id):
        Exception.__init__(self, 'Task "%s" does not exist.' % task_id)


class InvalidTaskTransition(Exception):

    def __init__(self, src, dest):
        Exception.__init__(self, 'Task transition from "%s" to "%s" does not exist.' % (src, dest))


class AmbiguousTaskTransition(Exception):

    def __init__(self, src, dest):
        message = 'More than one task transitions found from "%s" to "%s".' % (src, dest)
        Exception.__init__(self, message)


class InvalidState(Exception):

    def __init__(self, value):
        Exception.__init__(self, 'State "%s" is not valid.' % value)


class InvalidStateTransition(Exception):

    def __init__(self, old, new):
        Exception.__init__(self, 'State transition from "%s" to "%s" is invalid.' % (old, new))
