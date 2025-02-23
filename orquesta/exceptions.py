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

import logging

LOG = logging.getLogger(__name__)


class OrquestaException(Exception):
    pass


class PluginFactoryError(OrquestaException):
    pass


class ExpressionGrammarException(OrquestaException):
    pass


class ExpressionEvaluationException(OrquestaException):
    pass


class VariableUndefinedError(OrquestaException):
    def __init__(self, var):
        message = 'The variable "%s" is undefined.' % var
        super(VariableUndefinedError, self).__init__(message)


class VariableInaccessibleError(OrquestaException):
    def __init__(self, var):
        message = 'The variable "%s" is for internal use and inaccessible.' % var
        super(VariableInaccessibleError, self).__init__(message)


class SchemaDefinitionError(OrquestaException):
    pass


class SchemaIncompatibleError(OrquestaException):
    pass


class InvalidTask(OrquestaException):
    def __init__(self, task_id):
        message = 'Task "%s" does not exist.' % task_id
        super(InvalidTask, self).__init__(message)


class InvalidTaskTransition(OrquestaException):
    def __init__(self, src, dest):
        message = 'Task transition from "%s" to "%s" does not exist.' % (src, dest)
        super(InvalidTaskTransition, self).__init__(message)


class AmbiguousTaskTransition(OrquestaException):
    def __init__(self, src, dest):
        message = 'More than one task transitions found from "%s" to "%s".' % (src, dest)
        OrquestaException.__init__(self, message)


class InvalidEventType(OrquestaException):
    def __init__(self, type_name, event_name):
        message = 'Event type "%s" with event "%s" is not valid.' % (type_name, event_name)
        OrquestaException.__init__(self, message)


class InvalidEvent(OrquestaException):
    def __init__(self, value):
        message = 'Event "%s" is not valid.' % value
        super(InvalidEvent, self).__init__(message)


class InvalidStatus(OrquestaException):
    def __init__(self, value):
        message = 'Status "%s" is not valid.' % value
        super(InvalidStatus, self).__init__(message)


class InvalidStatusTransition(OrquestaException):
    def __init__(self, old, new):
        message = 'Status transition from "%s" to "%s" is invalid.' % (old, new)
        super(InvalidStatusTransition, self).__init__(message)


class InvalidTaskStatusTransition(OrquestaException):
    def __init__(self, status, event):
        message = 'Unable to process event "%s" for task in "%s" status.' % (event, status)
        super(InvalidTaskStatusTransition, self).__init__(message)


class InvalidWorkflowStatusTransition(OrquestaException):
    def __init__(self, status, event):
        message = 'Unable to process event "%s" for workflow in "%s" status.' % (event, status)
        super(InvalidWorkflowStatusTransition, self).__init__(message)


class InvalidTaskStateEntry(OrquestaException):
    def __init__(self, task_id):
        message = 'Task "%s" is not staged or has not started yet.' % task_id
        super(InvalidTaskStateEntry, self).__init__(message)


class WorkflowInspectionError(OrquestaException):
    def __init__(self, errors):
        message = "Workflow definition failed inspection."
        super(WorkflowInspectionError, self).__init__(message, errors)


class WorkflowContextError(OrquestaException):
    pass


class WorkflowLogEntryError(OrquestaException):
    pass


class WorkflowIsActiveAndNotRerunableError(OrquestaException):
    def __init__(self):
        message = "Unable to rerun workflow because it is not in a completed state."
        super(WorkflowIsActiveAndNotRerunableError, self).__init__(message)


class InvalidTaskRerunRequest(OrquestaException):
    def __init__(self, tasks):
        tasks_str = ""

        for task in tasks:
            tasks_str += ", " if tasks_str else ""
            tasks_str += "%s|%s" % (task.task_id, task.route)

        message = "Unable to rerun task|route(s) because it doesn't exist or isn't rerunnable: %s"
        super(InvalidTaskRerunRequest, self).__init__(message % tasks_str)


class UnreachableJoinError(OrquestaException):
    def __init__(self, task_id, route):
        message = 'The join task|route "%s|%s" is partially satisfied but unreachable.'
        super(UnreachableJoinError, self).__init__(message % (task_id, route))


class WorkflowRehearsalError(OrquestaException):
    pass
