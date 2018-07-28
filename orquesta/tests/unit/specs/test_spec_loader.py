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

from orquesta.specs import loader
from orquesta.specs import mock


class SpecLoaderTest(unittest.TestCase):

    def test_get_bad_module(self):
        self.assertRaises(ImportError, loader.get_spec_module, 'foobar')

    def test_get_module(self):
        self.assertEqual(loader.get_spec_module('mock'), mock)

    def test_get_spec(self):
        spec_module = loader.get_spec_module('mock')

        self.assertEqual(spec_module.WorkflowSpec.get_catalog(), 'mock')
        self.assertEqual(spec_module.WorkflowSpec, mock.WorkflowSpec)
