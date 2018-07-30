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

from orquesta.composers import base
from orquesta import graphing
from orquesta.specs import mock as specs


LOG = logging.getLogger(__name__)


class WorkflowComposer(base.WorkflowComposer):
    wf_spec_type = specs.WorkflowSpec

    @classmethod
    def compose(cls, spec):
        if not cls.wf_spec_type:
            raise TypeError('Undefined spec type for composer.')

        if not isinstance(spec, cls.wf_spec_type):
            raise TypeError('Unsupported spec type "%s".' % str(type(spec)))

        wf_graph = graphing.WorkflowGraph()

        return wf_graph
