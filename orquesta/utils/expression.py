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

import six


def format_error(expr_type, expr, exc, spec_path=None, schema_path=None):
    error = {
        "type": expr_type,
        "expression": expr,
        "message": exc if isinstance(exc, six.string_types) else str(getattr(exc, "message", exc)),
    }

    if spec_path:
        error["spec_path"] = spec_path

    if schema_path:
        error["schema_path"] = schema_path

    return error
