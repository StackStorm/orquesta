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

import abc
import logging
import six

from orquesta.utils import plugin as plugin_util


LOG = logging.getLogger(__name__)


def get_composer(catalog):
    return plugin_util.get_module("orquesta.composers", catalog)


@six.add_metaclass(abc.ABCMeta)
class WorkflowComposer(object):
    wf_spec_type = None

    @classmethod
    @abc.abstractmethod
    def compose(cls, spec):
        raise NotImplementedError()
