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

import chardet
import six

if six.PY2:
    import __builtin__  # pylint: disable=import-error


LOG = logging.getLogger(__name__)


def unescape(s):
    return s.decode("string_escape") if six.PY2 else bytes(s, "utf-8").decode("unicode_escape")


def encoding(s):
    return chardet.detect(s)["encoding"]


def unicode(s, force=False, encoding_type=None):
    if not isinstance(s, six.string_types) and not force:
        return s

    if six.PY2:
        if not force and (isinstance(s, __builtin__.unicode) or encoding(s) == "utf-8"):
            return s

        return __builtin__.unicode(s, encoding_type) if encoding_type else __builtin__.unicode(s)

    return str(s)
