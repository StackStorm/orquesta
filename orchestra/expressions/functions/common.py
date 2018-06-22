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

from os import environ
import json
import six

from orchestra import exceptions as exc


def json_(context, s):
    if isinstance(s, dict):
        return s

    if not isinstance(s, six.string_types):
        raise TypeError('Given object is not typeof string.')

    return json.loads(s)


def ctx_(context, key=None):
    if key and key not in context['__vars']:
        raise exc.VariableUndefinedError(key)

    return context['__vars'][key] if key else context['__vars']


def env_(context, var=None):
    if var is not None and not isinstance(var, six.string_types):
        raise TypeError('Given variale is not typeof string.')

    if var:
        return environ.get(var)

    return dict(environ)
