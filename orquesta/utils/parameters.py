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
import re
import six

from orquesta.expressions import base as expr_base


REGEX_VALUE_IN_BRACKETS = r"\[.*\]\s*"
REGEX_VALUE_IN_QUOTES = r"\"[^\"]*\"\s*"
REGEX_VALUE_IN_APOSTROPHES = r"'[^']*'\s*"
REGEX_FLOATING_NUMBER = r"[-]?\d*\.\d+"
REGEX_INTEGER = r"[-]?\d+"
REGEX_TRUE = r"(?i:true)" if six.PY3 else r"(?i)true"
REGEX_FALSE = r"(?i:false)" if six.PY3 else r"(?i)false"
REGEX_NULL = r"null"

# REGEX_FLOATING_NUMBER must go before REGEX_INTEGER
REGEX_INLINE_PARAM_VARIATIONS = [
    REGEX_VALUE_IN_BRACKETS,
    REGEX_VALUE_IN_QUOTES,
    REGEX_VALUE_IN_APOSTROPHES,
    REGEX_FLOATING_NUMBER,
    REGEX_INTEGER,
    REGEX_TRUE,
    REGEX_FALSE,
    REGEX_NULL,
]

REGEX_INLINE_PARAM_VARIATIONS.extend(
    [e._regex_pattern for e in expr_base.get_evaluators().values()]
)

REGEX_INLINE_PARAMS = r"([\w]+)=(%s)" % "|".join(REGEX_INLINE_PARAM_VARIATIONS)


def parse_inline_params(s, preserve_order=True):
    # Use a list to preserve order.
    params = [] if preserve_order else {}

    if s is None or not isinstance(s, six.string_types) or s == str():
        return params

    for k, v in re.findall(REGEX_INLINE_PARAMS, s):
        # Remove leading and trailing whitespaces.
        v = v.strip()

        quotes_in_param = bool(
            re.findall(REGEX_VALUE_IN_QUOTES, v) or re.findall(REGEX_VALUE_IN_APOSTROPHES, v)
        )

        # Remove leading and trailing double quotes.
        v = re.sub('^"', "", v)
        v = re.sub('"$', "", v)

        # Remove leading and trailing single quotes.
        v = re.sub("^'", "", v)
        v = re.sub("'$", "", v)

        quotes_in_string = False
        if v != "":
            # Check if param is a JSON string becuse it can be handled by json.loads()
            curly_brace_in_param = v[0] == "{" and v[-1] == "}"
            quotes_in_string = quotes_in_param and not curly_brace_in_param
        is_bool_value = v.lower() == "true" or v.lower() == "false"

        # Load string into dictionary.
        try:
            if not quotes_in_string:
                if is_bool_value:
                    v = v.lower()
                v = json.loads(v)
        except Exception:
            pass

        if preserve_order:
            params.append({k: v})
        else:
            params[k] = v

    return params
