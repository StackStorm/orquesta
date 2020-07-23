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

import threading

from stevedore import extension


_EXP_FUNC_CATALOG = None
_EXP_FUNC_CATALOG_LOCK = threading.Lock()


def load():
    global _EXP_FUNC_CATALOG
    global _EXP_FUNC_CATALOG_LOCK

    with _EXP_FUNC_CATALOG_LOCK:
        if _EXP_FUNC_CATALOG is None:
            _EXP_FUNC_CATALOG = {}

            mgr = extension.ExtensionManager(
                namespace="orquesta.expressions.functions", invoke_on_load=False
            )

            for name in mgr.names():
                _EXP_FUNC_CATALOG[name] = mgr[name].plugin

    return _EXP_FUNC_CATALOG
