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

import unittest

from orquesta import exceptions as exc
from orquesta.utils import plugin as plugin_util


class FakePlugin(object):
    pass


class PluginFactoryTest(unittest.TestCase):
    def test_get_instance(self):
        self.assertIsInstance(plugin_util.get_instance("orquesta.tests", "fake"), FakePlugin)

    def test_get_instance_failed(self):
        self.assertRaises(
            exc.PluginFactoryError, plugin_util.get_instance, "orquesta.tests", "foobar"
        )

    def test_get_module(self):
        self.assertEqual(plugin_util.get_module("orquesta.tests", "fake"), FakePlugin)

    def test_get_module_failed(self):
        self.assertRaises(
            exc.PluginFactoryError, plugin_util.get_module, "orquesta.tests", "foobar"
        )
