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

import datetime
import unittest

import dateutil.tz

from orquesta.utils import date


class DateTest(unittest.TestCase):

    def test_date_valid(self):
        self.assertFalse(date.valid(123))
        self.assertFalse(date.valid(False))
        self.assertFalse(date.valid('abcde'))
        self.assertTrue(date.valid(datetime.datetime.utcnow()))
        self.assertTrue(date.valid(str(datetime.datetime.utcnow())))
        self.assertTrue(date.valid('2015-01-01 12:00:01.000000Z'))
        self.assertTrue(date.valid('2015-01-01 12:00:01.000000+04'))
        self.assertTrue(date.valid('2015-01-01 12:00:01.000000+0600'))
        self.assertTrue(date.valid('2015-01-01 12:00:01.000000+08:30'))

    def test_date_parse(self):
        offset = dateutil.tz.tzoffset(None, 3600)
        expected = datetime.datetime(2015, 1, 1, 12, 0, 1, tzinfo=offset)
        dt_str = '2015-01-01T12:00:01.000000+01:00'
        self.assertEqual(expected, date.parse(dt_str))

    def test_date_format(self):
        offset = dateutil.tz.tzoffset(None, 3600)
        dt = datetime.datetime(2015, 1, 1, 12, 0, 1, tzinfo=offset)
        expected = '2015-01-01T12:00:01.000000+01:00'
        self.assertEqual(expected, date.format(dt))
