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
import re
import six

from orquesta.expressions import base as expr


REGEX_VALUE_IN_BRACKETS = '\[.*\]\s*'
REGEX_VALUE_IN_QUOTES = '\"[^\"]*\"\s*'
REGEX_VALUE_IN_APOSTROPHES = "'[^']*'\s*"
REGEX_FLOATING_NUMBER = '[-]?\d*\.\d+'
REGEX_INTEGER = '[-]?\d+'
REGEX_TRUE = 'true'
REGEX_FALSE = 'false'
REGEX_NULL = 'null'

# REGEX_FLOATING_NUMBER must go before REGEX_INTEGER
REGEX_INLINE_PARAM_VARIATIONS = [
    REGEX_VALUE_IN_BRACKETS,
    REGEX_VALUE_IN_QUOTES,
    REGEX_VALUE_IN_APOSTROPHES,
    REGEX_FLOATING_NUMBER,
    REGEX_INTEGER,
    REGEX_TRUE,
    REGEX_FALSE,
    REGEX_NULL
]

REGEX_INLINE_PARAM_VARIATIONS.extend([e._regex_pattern for e in expr.get_evaluators().values()])
REGEX_INLINE_PARAMS = '([\w]+)=(%s)' % '|'.join(REGEX_INLINE_PARAM_VARIATIONS)


def parse_inline_params(s, preserve_order=True):
    # Use a list to preserve order.
    params = [] if preserve_order else {}

    if s is None or not isinstance(s, six.string_types) or s == str():
        return params

    for k, v in re.findall(REGEX_INLINE_PARAMS, s):
        # Remove leading and trailing whitespaces.
        v = v.strip()

        # Remove leading and trailing double quotes.
        v = re.sub('^"', '', v)
        v = re.sub('"$', '', v)

        # Remove leading and trailing single quotes.
        v = re.sub("^'", '', v)
        v = re.sub("'$", '', v)

        # Load string into dictionary.
        try:
            v = json.loads(v)
        except Exception:
            pass

        if preserve_order:
            params.append({k: v})
        else:
            params[k] = v

    return params
