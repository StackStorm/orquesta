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

# pylint: disable=no-member

import datetime
import logging
import re
import six

import dateutil.parser


LOG = logging.getLogger(__name__)


ISO8601_FORMAT = r"%Y-%m-%dT%H:%M:%S"
ISO8601_FORMAT_USEC = ISO8601_FORMAT + r".%f"
ISO8601_REGEX = r"\d{4}\-\d{2}\-\d{2}(\s|T)\d{2}:\d{2}:\d{2}(\.\d{3,6})"
ISO8601_REGEX_NAIVE = r"^%s$" % ISO8601_REGEX
ISO8601_REGEX_UTC = r"^%s?(Z|\+00|\+0000|\+00:00)$" % ISO8601_REGEX
ISO8601_REGEX_TZ = r"^%s?(Z|\+\d{2}|\+\d{4}|\+\d{2}:\d{2})$" % ISO8601_REGEX


def valid(v):
    is_dt_obj = isinstance(v, datetime.datetime)
    is_dt_str = isinstance(v, six.string_types) and re.match(ISO8601_REGEX_NAIVE, v)
    is_dt_str_tz = isinstance(v, six.string_types) and re.match(ISO8601_REGEX_TZ, v)

    return is_dt_obj or is_dt_str or is_dt_str_tz


def format(dt, usec=True, offset=True):
    if isinstance(dt, six.string_types):
        dt = parse(dt)

    fmt = ISO8601_FORMAT_USEC if usec else ISO8601_FORMAT

    if offset:
        ost = dt.strftime("%z")
        ost = (ost[:3] + ":" + ost[3:]) if ost else "+00:00"
    else:
        tz = dt.tzinfo.tzname(dt) if dt.tzinfo else "UTC"
        ost = "Z" if tz == "UTC" else tz

    return dt.strftime(fmt) + ost


def parse(v):
    return dateutil.parser.parse(str(v))
