# -*- coding: utf-8 -*-

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
import unittest

from orquesta.utils import strings


class StringsTest(unittest.TestCase):

    def test_unescape(self):
        self.assertEqual(strings.unescape('foobar'), 'foobar')
        self.assertEqual(strings.unescape('\\\\foobar'), '\\foobar')

    def test_unicode(self):
        self.assertEqual(strings.unicode(123), 123)
        self.assertEqual(strings.unicode('foobar'), 'foobar')
        self.assertEqual(strings.unicode(unicode('foobar') if six.PY2 else str('foobar')), 'foobar')
        self.assertEqual(strings.unicode('鐵甲奇俠'), '鐵甲奇俠')
        self.assertEqual(strings.unicode('\xe9\x90\xb5\xe7\x94\xb2'), '\xe9\x90\xb5\xe7\x94\xb2')

    def test_unicode_force(self):
        self.assertEqual(strings.unicode(123, force=True), '123')
        self.assertEqual(strings.unicode(123.45, force=True), '123.45')
        self.assertEqual(strings.unicode(True, force=True), 'True')
        self.assertEqual(strings.unicode(None, force=True), 'None')
        self.assertEqual(strings.unicode([1, 2, 3], force=True), '[1, 2, 3]')
        self.assertEqual(strings.unicode({'k': 'v'}, force=True), "{'k': 'v'}")
