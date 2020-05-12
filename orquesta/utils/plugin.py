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

from stevedore import driver

from orquesta import exceptions as exc


LOG = logging.getLogger(__name__)


def get_module(namespace, name):
    try:
        mgr = driver.DriverManager(namespace=namespace, name=name, invoke_on_load=False)
    except RuntimeError as e:
        raise exc.PluginFactoryError("Unable to load plugin %s.%s. %s" % (namespace, name, str(e)))

    return mgr.driver


def get_instance(namespace, name, *args, **kwargs):
    try:
        mgr = driver.DriverManager(
            namespace=namespace,
            name=name,
            invoke_on_load=True,
            invoke_args=args,
            invoke_kwds=kwargs,
        )
    except RuntimeError as e:
        raise exc.PluginFactoryError("Unable to load plugin %s.%s. %s" % (namespace, name, str(e)))

    return mgr.driver
