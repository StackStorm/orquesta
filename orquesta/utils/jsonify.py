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

import datetime
import logging

import six

from orquesta.utils import date


LOG = logging.getLogger(__name__)


SERIALIZABLE_TYPES = (
    six.string_types,
    bool,
    datetime.datetime,
    dict,
    float,
    int,
    list,
)


def serialize(obj):
    doc = {}

    for k, v in six.iteritems(obj.__dict__):
        if isinstance(v, SERIALIZABLE_TYPES):
            doc[k] = date.format(v) if date.valid(v) else v

    return doc


def deserialize(obj_type, data):
    obj = obj_type()

    for k, v in six.iteritems(data):
        if isinstance(v, SERIALIZABLE_TYPES):
            v = date.parse(v) if date.valid(v) else v
            setattr(obj, k, v)

    return obj
