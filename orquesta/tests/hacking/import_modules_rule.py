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

import imp
import inspect
import sys
import traceback

from hacking import core


MODULES_CACHE = dict((mod, True) for mod in tuple(sys.modules.keys()) + sys.builtin_module_names)


@core.flake8ext
@core.off_by_default
def check_module_only(logical_line, filename, noqa):
    """Check imports.

    Imports should be modules only.

    Examples:
    Okay: from orquesta import conducting
    Okay: from orquesta import states
    Okay: from orquesta.utils import date
    O101: from orquesta.conducting import WorkflowConductor
    O101: from orquesta.states import REQUESTED
    O101: from orquesta.utils.date import parse
    """

    if noqa:
        return

    def check_is_module(mod, search_path=sys.path):
        mod = mod.replace("(", "")  # Ignore parentheses

        for finder in sys.meta_path:
            if finder.find_module(mod) is not None:
                return True

        try:
            mod_name = mod

            while "." in mod_name:
                pack_name, _sep, mod_name = mod_name.partition(".")
                f, p, d = imp.find_module(pack_name, search_path)
                search_path = [p]

            imp.find_module(mod_name, search_path)
        except ImportError:
            try:
                # Handle namespace modules
                if "." in mod:
                    pack_name, mod_name = mod.rsplit(".", 1)
                    __import__(pack_name, fromlist=[mod_name])
                else:
                    __import__(mod)
            except ImportError:
                # Import error here means the thing is not importable in
                # current environment, either because of missing dependency,
                # typo in code being checked, or any other reason. Anyway, we
                # have no means to know if it is module or not, so we return
                # True to avoid false positives.
                return True
            except Exception:
                # Skip stack trace on unexpected import error,
                # log and continue.
                traceback.print_exc()
                return False
            else:
                # The line was imported. If it is a module, it must be there.
                # If it is not there under its own name, look to see if it is
                # a module redirection import..
                if mod in sys.modules:
                    return True
                else:
                    pack_name, _sep, mod_name = mod.rpartition(".")
                    if pack_name in sys.modules:
                        _mod = getattr(sys.modules[pack_name], mod_name, None)
                        return inspect.ismodule(_mod)
                    return False

        return True

    def is_module(mod):
        if mod not in MODULES_CACHE:
            MODULES_CACHE[mod] = check_is_module(mod)

        return MODULES_CACHE[mod]

    parts = logical_line.split()

    if (
        len(parts) > 3
        and parts[0] == "from"
        and parts[1] != "__future__"
        and not core.is_import_exception(parts[1])
        and not is_module(".".join([parts[1], parts[3]]))
    ):
        yield 0, "O101 import must be module only"
