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

import json
import six

from orquesta import exceptions as exc


def json_(s):
    if isinstance(s, dict):
        return s

    if not isinstance(s, six.string_types):
        raise TypeError("Given object is not typeof string.")

    return json.loads(s)


def zip_(*args, **kwargs):
    args = [list() if arg is None else arg for arg in args]

    if len(args) == 1:
        return args[0]

    pad_with = kwargs.pop("pad", None)

    return list(six.moves.zip_longest(*args, fillvalue=pad_with))


def ctx_(context, key=None):
    if key:
        if key not in context["__vars"]:
            raise exc.VariableUndefinedError(key)

        if key in context["__vars"] and key.startswith("__"):
            raise exc.VariableInaccessibleError(key)

        return context["__vars"][key]
    else:
        return {k: v for k, v in six.iteritems(context["__vars"]) if not k.startswith("__")}
