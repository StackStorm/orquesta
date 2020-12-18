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

from hacking import core


CODE = "O102"

REQS = {
    "orquesta.composers.base": "comp_base",
    "orquesta.conducting": None,
    "orquesta.constants": None,
    "orquesta.events": None,
    "orquesta.graphing": None,
    "orquesta.exceptions": "exc",
    "orquesta.expressions.base": "expr_base",
    "orquesta.expressions.functions.base": "func_base",
    "orquesta.expressions.jinja": "jinja_expr",
    "orquesta.expressions.yql": "yaql_expr",
    "orquesta.machines": None,
    "orquesta.specs.base": "spec_base",
    "orquesta.specs.loader": "spec_loader",
    "orquesta.specs.mistral": "mistral_specs",
    "orquesta.specs.mistral.v2": "mistral_v2_specs",
    "orquesta.specs.mock": "mock_specs",
    "orquesta.specs.mock.models": "mock_models",
    "orquesta.specs.native": "native_specs",
    "orquesta.specs.native.v1": "native_v1_specs",
    "orquesta.specs.native.v1.models": "native_v1_models",
    "orquesta.specs.types": "spec_types",
    "orquesta.statuses": None,
    "orquesta.tests.fixtures.loader": "fixture_loader",
    "orquesta.tests.unit.base": "test_base",
    "orquesta.tests.unit.specs.base": "test_specs",
    "orquesta.utils.context": "ctx_util",
    "orquesta.utils.date": "date_util",
    "orquesta.utils.dictionary": "dict_util",
    "orquesta.utils.expression": "expr_util",
    "orquesta.utils.jsonify": "json_util",
    "orquesta.utils.parameters": "args_util",
    "orquesta.utils.plugin": "plugin_util",
    "orquesta.utils.schema": "schema_util",
    "orquesta.utils.specs": "spec_util",
    "orquesta.utils.strings": "str_util",
}


def get_alias(logical_line):

    parts = logical_line.split()

    if (
        len(parts) > 3
        and parts[0] == "from"
        and parts[1] != "__future__"
        and not core.is_import_exception(parts[1])
    ):

        # from path.to.module import module
        if len(parts) == 4:
            return ".".join([parts[1], parts[3]]), None

        # from path.to.module import module as alias
        if len(parts) > 5 and parts[4] == "as":
            return ".".join([parts[1], parts[3]]), parts[5]

    return None


@core.flake8ext
@core.off_by_default
def check_alias_naming(logical_line, filename, noqa):
    """Check import alias.

    To ensure code consistency, check if a module must use a specific alias.

    Examples:
    """

    if noqa:
        return

    alias = get_alias(logical_line)

    # Dictate import from using alias.
    if alias and alias[0] in REQS and alias[1] and not REQS[alias[0]]:
        message = "cannot use alias for import '%s'"
        yield 0, "%s %s" % (CODE, message % logical_line)
    # Dictate import to use alias.
    elif alias and alias[0] in REQS and not alias[1] and REQS[alias[0]]:
        message = "import '%s' must use alias '%s'"
        yield 0, "%s %s" % (CODE, message % (logical_line, REQS[alias[0]]))
    # Dictate import to use specific alias.
    elif alias and alias[0] in REQS and alias[1] and REQS[alias[0]] and alias[1] != REQS[alias[0]]:
        message = "import '%s' must use alias '%s'"
        yield 0, "%s %s" % (CODE, message % (logical_line, REQS[alias[0]]))
