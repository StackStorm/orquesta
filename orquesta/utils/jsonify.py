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

import copy
import datetime
import logging
import six
import ujson

from orquesta.utils import date as date_util


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
            doc[k] = date_util.format(v) if date_util.valid(v) else v

    return doc


def deserialize(obj_type, data):
    obj = obj_type()

    for k, v in six.iteritems(data):
        if isinstance(v, SERIALIZABLE_TYPES):
            v = date_util.parse(v) if date_util.valid(v) else v
            setattr(obj, k, v)

    return obj


def deepcopy(value):
    # NOTE: ujson round-trip is up to 10 times faster on smaller and larger dicts compared
    # to copy.deepcopy(), but it has some edge cases with non-simple types such as datetimes.
    try:
        value = ujson.loads(ujson.dumps(value))  # pylint: disable=no-member
    except (OverflowError, ValueError, TypeError):
        # NOTE: ujson doesn't support 5 or 6 bytes utf-8 sequences or
        # non-JSON serializable object so we fallback to copy.deepcopy.
        value = copy.deepcopy(value)

    return value
