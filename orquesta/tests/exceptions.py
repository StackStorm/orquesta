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


from orquesta.exceptions import OrquestaException


class SpecException(OrquestaException):
    pass


class IncorrectSpec(SpecException):
    pass


class WorkflowSpecError(SpecException):
    pass


class FixtureMockSpecError(OrquestaException):
    pass


class OrquestaFixtureTestError(OrquestaException):
    pass


class WorkflowTestException(OrquestaException):
    pass


class MockConductorError(WorkflowTestException):
    pass


class TaskEquality(WorkflowTestException):
    pass


class RouteEquality(WorkflowTestException):
    pass


class StatusEquality(WorkflowTestException):
    pass


class OutputEquality(WorkflowTestException):
    pass


class TermsEquality(WorkflowTestException):
    pass
