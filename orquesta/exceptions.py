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


LOG = logging.getLogger(__name__)


class PluginFactoryError(Exception):
    pass


class ExpressionGrammarException(Exception):
    pass


class ExpressionEvaluationException(Exception):
    pass


class VariableUndefinedError(Exception):

    def __init__(self, var):
        Exception.__init__(self, 'The variable "%s" is undefined.' % var)


class VariableInaccessibleError(Exception):

    def __init__(self, var):
        Exception.__init__(self, 'The variable "%s" is for internal use and inaccessible.' % var)


class SchemaDefinitionError(Exception):
    pass


class SchemaIncompatibleError(Exception):
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


class InvalidEventType(Exception):

    def __init__(self, type_name, event_name):
        message = 'Event type "%s" with event "%s" is not valid.' % (type_name, event_name)
        Exception.__init__(self, message)


class InvalidEvent(Exception):

    def __init__(self, value):
        Exception.__init__(self, 'Event "%s" is not valid.' % value)


class InvalidStatus(Exception):

    def __init__(self, value):
        Exception.__init__(self, 'Status "%s" is not valid.' % value)


class InvalidStatusTransition(Exception):

    def __init__(self, old, new):
        Exception.__init__(self, 'Status transition from "%s" to "%s" is invalid.' % (old, new))


class InvalidTaskStatusTransition(Exception):

    def __init__(self, status, event):
        message = 'Unable to process event "%s" for task in "%s" status.' % (event, status)
        Exception.__init__(self, message)


class InvalidWorkflowStatusTransition(Exception):

    def __init__(self, status, event):
        message = 'Unable to process event "%s" for workflow in "%s" status.' % (event, status)
        Exception.__init__(self, message)


class InvalidTaskStateEntry(Exception):

    def __init__(self, task_id):
        Exception.__init__(self, 'Task "%s" is not staged or has not started yet.' % task_id)


class WorkflowInspectionError(Exception):
    def __init__(self, errors):
        Exception.__init__(self, 'Workflow definition failed inspection.', errors)


class WorkflowContextError(Exception):
    pass


class WorkflowLogEntryError(Exception):
    pass
